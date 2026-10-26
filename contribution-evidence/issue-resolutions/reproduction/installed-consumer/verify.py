"""Verify installed artifacts and real tests outside the source checkout."""

import hashlib
import importlib.metadata
import json
import subprocess
import sys
import zipfile
from pathlib import Path

import lance_csr
from lance_csr import _internal


def digest(stream):
    return hashlib.file_digest(stream, "sha256").hexdigest()


wheel = Path(sys.argv[1])
package = Path(lance_csr.__file__).resolve().parent
extension = Path(_internal.__file__).resolve()
assert "site-packages" in extension.parts, extension
assert package == extension.parent, (package, extension)
typing = []
with zipfile.ZipFile(wheel) as archive:
    members = [
        name
        for name in archive.namelist()
        if name.startswith("lance_csr/") and not name.endswith("/")
    ]
    for member in members:
        installed = package / Path(member).relative_to("lance_csr")
        with archive.open(member) as source, installed.open("rb") as actual:
            assert digest(source) == digest(actual), member
        if member.endswith((".pyi", "/py.typed")):
            typing.append(member)
assert sorted(typing) == [
    "lance_csr/_internal/__init__.pyi",
    "lance_csr/_internal/graph.pyi",
    "lance_csr/py.typed",
]
print(
    json.dumps(
        {
            "python": sys.version,
            "extension": str(extension),
            "lance-csr": importlib.metadata.version("lance-csr"),
            "pyarrow": importlib.metadata.version("pyarrow"),
            "pylance": importlib.metadata.version("pylance"),
            "installed_typing_matches_archive": typing,
        }
    ),
    flush=True,
)
subprocess.run([sys.executable, "-I", "check_wheel.py"], check=True)
subprocess.run([sys.executable, "-I", "-m", "pytest", "tests", "-q"], check=True)
subprocess.run(
    [
        sys.executable,
        "-I",
        "-m",
        "pyright",
        "--pythonpath",
        sys.executable,
        "--project",
        "pyrightconfig.json",
    ],
    check=True,
)
