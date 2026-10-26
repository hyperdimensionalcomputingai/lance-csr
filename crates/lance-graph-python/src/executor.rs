// Copyright 2023 Lance Developers.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use futures::Future;
use pyo3::{
    exceptions::{PyTimeoutError, PyValueError},
    PyResult, Python,
};
use std::time::Duration;

pub(crate) const QUERY_TIMEOUT_MESSAGE: &str = "Query timeout exceeded";

/// One absolute deadline shared by planning, execution and all streamed reads.
#[derive(Clone, Copy)]
pub(crate) struct QueryDeadline(Option<tokio::time::Instant>);

impl QueryDeadline {
    pub(crate) fn new(timeout: Option<f64>) -> PyResult<Self> {
        let Some(seconds) = timeout else {
            return Ok(Self(None));
        };
        let duration = Duration::try_from_secs_f64(seconds).map_err(|_| {
            PyValueError::new_err("timeout must be a finite nonnegative number of seconds")
        })?;
        let instant = tokio::time::Instant::now()
            .checked_add(duration)
            .ok_or_else(|| PyValueError::new_err("timeout exceeds the supported deadline range"))?;
        Ok(Self(Some(instant)))
    }

    pub(crate) fn instant(self) -> Option<tokio::time::Instant> {
        self.0
    }

    pub(crate) fn check(self) -> PyResult<()> {
        if self
            .0
            .is_some_and(|deadline| tokio::time::Instant::now() >= deadline)
        {
            Err(PyTimeoutError::new_err(QUERY_TIMEOUT_MESSAGE))
        } else {
            Ok(())
        }
    }

    async fn expired(self) {
        match self.0 {
            Some(deadline) => tokio::time::sleep_until(deadline).await,
            None => std::future::pending::<()>().await,
        }
    }
}

pub const SIGNAL_CHECK_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);

/// A wrapper around tokio runtime.
///
/// This is used to spawn tasks in the background and wait synchronously for them
/// to complete. This is important for cases where we want to avoid nested
/// block_on() calls.
///
/// The methods also make sure that the GIL is released before spawning the task.
pub struct BackgroundExecutor {
    pub runtime: tokio::runtime::Runtime,
}

impl BackgroundExecutor {
    /// Creates a tokio runtime and spawns a thread to run it.
    pub fn new() -> Self {
        // Create a new Runtime to run tasks
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("lance_background_thread")
            .build()
            .expect("Creating Tokio runtime");

        Self { runtime }
    }

    /// Block on a future and wait for it to complete.
    ///
    /// This helper method also frees the GIL before blocking.
    ///
    /// This method is NOT safe to use with inputs that may reference a Rust async
    /// runtime. If the future references an async runtime, it will panic on an
    /// error: "Cannot start a runtime from within a runtime."
    pub fn block_on<F: Future + Send>(
        &self,
        py: Option<Python<'_>>,
        future: F,
    ) -> PyResult<F::Output>
    where
        F::Output: Send,
    {
        self.block_on_until(py, QueryDeadline(None), future)
    }

    /// Apply a query deadline without changing unrelated executor callers.
    pub(crate) fn block_on_until<F: Future + Send>(
        &self,
        py: Option<Python<'_>>,
        deadline: QueryDeadline,
        future: F,
    ) -> PyResult<F::Output>
    where
        F::Output: Send,
    {
        deadline.check()?;
        let future = lance_graph::query::with_execution_deadline(deadline.instant(), future);
        let future = Self::result_or_interrupt(future, deadline);
        if let Some(py) = py {
            py.detach(move || self.runtime.block_on(future))
        } else {
            // Python::attach is a no-op if the GIL is already held by the thread.
            Python::attach(|py| py.detach(|| self.runtime.block_on(future)))
        }
    }

    async fn result_or_interrupt<F>(future: F, deadline: QueryDeadline) -> PyResult<F::Output>
    where
        F: Future + Send,
        F::Output: Send,
    {
        let interrupt_future = async {
            loop {
                // Check for keyboard interrupts
                match Python::attach(|py| py.check_signals()) {
                    Ok(_) => {
                        // Wait for 100ms before checking signals again
                        tokio::time::sleep(SIGNAL_CHECK_INTERVAL).await;
                    }
                    Err(err) => return Err(err),
                }
            }
        };

        tokio::select! {
            biased;
            _ = deadline.expired() => Err(PyTimeoutError::new_err(QUERY_TIMEOUT_MESSAGE)),
            result = future => { deadline.check()?; Ok(result) },
            err = interrupt_future => err,
        }
    }
}
