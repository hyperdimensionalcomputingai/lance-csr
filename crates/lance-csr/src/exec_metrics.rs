//! Metrics for lance-csr's extension operators, which `EXPLAIN ANALYZE` prints beside
//! DataFusion's own.
//!
//! An operator wraps its input with [`time_input`] and its output with [`measure`]. That records
//! `output_rows` and `elapsed_compute`, the time spent in the operator's own polls minus the time
//! spent polling its input, as DataFusion's operators count it.
//!
//! shortcut: work done on other tasks (spawned or blocking) isn't counted, and child plans an
//! operator runs itself (`ApplyExec`'s subquery, `PathMatchExec`'s side inputs) count as its own
//! time. Time those separately if a profile needs to tell them apart.

use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::common::Result;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, Time};
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use futures::Stream;

/// Wrap an operator's input so that the time spent polling it can be left out of the operator's
/// `elapsed_compute`. Pass the returned [`Time`] to [`measure`].
pub(crate) fn time_input(input: SendableRecordBatchStream) -> (SendableRecordBatchStream, Time) {
    let time = Time::new();
    let timed = Timed {
        inner: input,
        time: time.clone(),
    };
    (Box::pin(timed), time)
}

/// Wrap an operator's output stream for `partition`, recording its metrics in `metrics`.
pub(crate) fn measure(
    output: SendableRecordBatchStream,
    metrics: &ExecutionPlanMetricsSet,
    partition: usize,
    input_time: Time,
) -> SendableRecordBatchStream {
    Box::pin(Measured {
        inner: output,
        baseline: BaselineMetrics::new(metrics, partition),
        input_time,
    })
}

struct Timed {
    inner: SendableRecordBatchStream,
    time: Time,
}

impl Stream for Timed {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let start = Instant::now();
        let poll = self.inner.as_mut().poll_next(cx);
        self.time.add_elapsed(start);
        poll
    }
}

impl RecordBatchStream for Timed {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

struct Measured {
    inner: SendableRecordBatchStream,
    baseline: BaselineMetrics,
    input_time: Time,
}

impl Stream for Measured {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let (start, input_before) = (Instant::now(), self.input_time.value());
        let poll = self.inner.as_mut().poll_next(cx);
        let input = self.input_time.value().saturating_sub(input_before) as u128;
        let own = start.elapsed().as_nanos().saturating_sub(input);
        self.baseline
            .elapsed_compute()
            .add_duration(Duration::from_nanos(own as u64));
        self.baseline.record_poll(poll)
    }
}

impl RecordBatchStream for Measured {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use futures::StreamExt;
    use std::sync::Arc;

    /// Rows are counted and the input's own poll time is left out of `elapsed_compute`.
    #[tokio::test]
    async fn counts_rows_and_excludes_input_time() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let slow = futures::stream::iter(vec![Ok(batch.clone()), Ok(batch)]).map(|b| {
            std::thread::sleep(Duration::from_millis(20));
            b
        });
        let input: SendableRecordBatchStream =
            Box::pin(RecordBatchStreamAdapter::new(schema.clone(), slow));
        let (input, input_time) = time_input(input);
        let metrics = ExecutionPlanMetricsSet::new();
        let output = measure(input, &metrics, 0, input_time.clone());
        let batches: Vec<_> = output.collect().await;
        assert_eq!(batches.len(), 2);
        let set = metrics.clone_inner();
        assert_eq!(set.output_rows(), Some(6));
        assert!(input_time.value() >= 40_000_000);
        assert!(set.elapsed_compute().unwrap() < 20_000_000, "{set:?}");
    }
}
