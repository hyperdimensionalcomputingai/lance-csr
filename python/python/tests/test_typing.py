"""Keep shipped stubs aligned with the compiled public API."""

import ast
import inspect
from pathlib import Path

import lance_csr
import pytest
from lance_csr import _internal


def stub_classes():
    """Read the installed stubs, so the same check also exercises a built wheel."""
    package = Path(lance_csr.__file__).parent
    assert (package / "py.typed").is_file()
    tree = ast.parse((package / "_internal" / "graph.pyi").read_text())
    return {
        node.name: node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and not node.name.startswith("_")
    }


def public_members(node):
    return {
        item.name if isinstance(item, ast.FunctionDef) else item.target.id: item
        for item in node.body
        if (isinstance(item, ast.FunctionDef) and not item.name.startswith("_"))
        or (
            isinstance(item, ast.AnnAssign)
            and isinstance(item.target, ast.Name)
            and not item.target.id.startswith("_")
        )
    }


def stub_signature(node):
    """Compare Python-callable names, kinds and defaults, ignoring annotations/self."""
    args = node.args
    positional = [*args.posonlyargs, *args.args]
    defaults = [inspect.Parameter.empty] * (len(positional) - len(args.defaults))
    defaults += [ast.literal_eval(value) for value in args.defaults]
    parameters = []
    for index, (arg, default) in enumerate(zip(positional, defaults, strict=True)):
        if arg.arg == "self":
            continue
        kind = (
            inspect.Parameter.POSITIONAL_ONLY
            if index < len(args.posonlyargs)
            else inspect.Parameter.POSITIONAL_OR_KEYWORD
        )
        parameters.append(inspect.Parameter(arg.arg, kind, default=default))
    if args.vararg:
        parameters.append(
            inspect.Parameter(args.vararg.arg, inspect.Parameter.VAR_POSITIONAL)
        )
    for arg, default in zip(args.kwonlyargs, args.kw_defaults, strict=True):
        parameters.append(
            inspect.Parameter(
                arg.arg,
                inspect.Parameter.KEYWORD_ONLY,
                default=inspect.Parameter.empty
                if default is None
                else ast.literal_eval(default),
            )
        )
    if args.kwarg:
        parameters.append(
            inspect.Parameter(args.kwarg.arg, inspect.Parameter.VAR_KEYWORD)
        )
    return inspect.Signature(parameters)


def runtime_signature(value):
    return inspect.Signature(
        [
            parameter.replace(annotation=inspect.Parameter.empty)
            for parameter in inspect.signature(value).parameters.values()
            if parameter.name != "self"
        ]
    )


def check_class(node, runtime):
    members = public_members(node)
    actual = {name for name in vars(runtime) if not name.startswith("_")}
    assert actual == members.keys(), f"{node.name}: runtime/stub public members differ"
    for name, member in members.items():
        value = getattr(runtime, name)
        if isinstance(member, ast.AnnAssign):
            assert isinstance(value, runtime), (
                f"{node.name}.{name}: expected enum member"
            )
        elif any(
            isinstance(d, ast.Name) and d.id == "property"
            for d in member.decorator_list
        ):
            assert inspect.isgetsetdescriptor(value), (
                f"{node.name}.{name}: expected property"
            )
        else:
            assert runtime_signature(value) == stub_signature(member), (
                f"{node.name}.{name}"
            )
    constructor = next(
        (
            item
            for item in node.body
            if isinstance(item, ast.FunctionDef) and item.name == "__init__"
        ),
        None,
    )
    if constructor is not None:
        assert runtime_signature(runtime) == stub_signature(constructor), node.name


def test_public_stubs_match_compiled_extension():
    classes = stub_classes()
    assert set(lance_csr.__all__) == classes.keys()
    assert {
        name for name in vars(_internal.graph) if not name.startswith("_")
    } == classes.keys()
    for name, node in classes.items():
        runtime = getattr(_internal.graph, name)
        assert getattr(lance_csr, name) is runtime
        check_class(node, runtime)


def test_stub_check_detects_missing_method():
    node = stub_classes()["CypherEngine"]
    node.body = [
        item
        for item in node.body
        if not isinstance(item, ast.FunctionDef) or item.name != "refresh"
    ]
    with pytest.raises(AssertionError, match="public members differ"):
        check_class(node, lance_csr.CypherEngine)


def test_stub_check_detects_changed_default():
    node = stub_classes()["CypherEngine"]
    execute = next(
        item
        for item in node.body
        if isinstance(item, ast.FunctionDef) and item.name == "execute"
    )
    execute.args.defaults = [ast.Constant(False)]
    with pytest.raises(AssertionError, match="CypherEngine.execute"):
        check_class(node, lance_csr.CypherEngine)
