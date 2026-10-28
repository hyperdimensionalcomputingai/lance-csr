//! The private compiled module behind the `lance_csr` Python package.
//!
//! Users see the Python classes in `python/python/lance_csr/`, which wrap these and
//! carry the docstrings and type hints. Document Python behaviour there, not here.

use std::sync::LazyLock;

use pyo3::prelude::*;

mod executor;
mod graph;
mod namespace;

pub(crate) static RT: LazyLock<executor::BackgroundExecutor> =
    LazyLock::new(executor::BackgroundExecutor::new);

#[pymodule]
fn _internal(py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    graph::register_graph_module(py, m)?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
