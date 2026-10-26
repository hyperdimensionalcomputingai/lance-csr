// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Distinct-count statistics for join-key columns, so DataFusion sizes joins correctly.
//!
//! DataFusion 54 estimates an equi-join's output as `L × R / max(NDV_l, NDV_r)`.
//! When a column reports no distinct count (true for every column of `MemTable` and
//! Lance's `LanceTableProvider`), it substitutes the row count. For a graph join chain
//! that turns a multi-million-row intermediate into an estimate of a few thousand
//! rows, under the 128k-row `CollectLeft` threshold, and DataFusion then builds that
//! intermediate as one single-threaded hash table.
//!
//! [`KeyStatisticsProvider`] wraps a table and reports known distinct counts for its
//! node-id and edge-endpoint columns; everything else is the inner table's.
//! [`KeyStatisticsExec`] is the scan-side wrapper. It is transparent like
//! DataFusion's own `CooperativeExec`: filters, projections, limits and sort pushdown
//! pass through to the inner scan unchanged.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{ScanArgs, ScanResult, Session};
use datafusion::common::config::ConfigOptions;
use datafusion::common::stats::Precision;
use datafusion::common::{Constraints, Result, Statistics};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterDescription, FilterPushdownPhase, FilterPushdownPropagation,
};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};

/// A table that reports `distinct` (column name -> distinct non-null values) as
/// column statistics, on top of whatever the inner table reports.
///
/// The counts are estimates for planning only; wrong counts give worse plans, never
/// wrong results.
#[derive(Debug)]
pub struct KeyStatisticsProvider {
    inner: Arc<dyn TableProvider>,
    distinct: HashMap<String, usize>,
}

impl KeyStatisticsProvider {
    pub fn new(inner: Arc<dyn TableProvider>, distinct: HashMap<String, usize>) -> Self {
        Self { inner, distinct }
    }
}

#[async_trait]
impl TableProvider for KeyStatisticsProvider {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.inner.constraints()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = self.inner.scan(state, projection, filters, limit).await?;
        let divisor = equality_divisor(filters, &self.distinct);
        Ok(KeyStatisticsExec::wrap(plan, &self.distinct, divisor))
    }

    async fn scan_with_args<'a>(
        &self,
        state: &dyn Session,
        args: ScanArgs<'a>,
    ) -> Result<ScanResult> {
        let divisor = equality_divisor(args.filters().unwrap_or_default(), &self.distinct);
        let plan = self.inner.scan_with_args(state, args).await?.into_inner();
        Ok(ScanResult::new(KeyStatisticsExec::wrap(
            plan,
            &self.distinct,
            divisor,
        )))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<Statistics> {
        self.inner.statistics()
    }
}

/// Row-count divisor for filters pushed into the scan: each `column = literal`
/// conjunct on a column with a known distinct count divides the rows by that count,
/// as DataFusion's `FilterExec` estimates equality. Needed because Lance applies
/// pushed filters exactly, so no `FilterExec` is left to estimate them and the scan
/// reports the whole table (a lookup by id looked like 10k rows instead of 1).
/// Other filters are ignored, which overestimates.
fn equality_divisor(filters: &[Expr], distinct: &HashMap<String, usize>) -> usize {
    use datafusion::logical_expr::utils::split_conjunction;
    use datafusion::logical_expr::{BinaryExpr, Operator};
    let mut divisor = 1usize;
    for conjunct in filters.iter().flat_map(split_conjunction) {
        let Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Eq,
            right,
        }) = conjunct
        else {
            continue;
        };
        let column = match (left.as_ref(), right.as_ref()) {
            (Expr::Column(c), Expr::Literal(..)) | (Expr::Literal(..), Expr::Column(c)) => c,
            _ => continue,
        };
        if let Some(&n) = distinct.get(&column.name) {
            divisor = divisor.saturating_mul(n.max(1));
        }
    }
    divisor
}

/// Scan wrapper that overrides `distinct_count` for the configured columns.
#[derive(Debug)]
pub struct KeyStatisticsExec {
    input: Arc<dyn ExecutionPlan>,
    /// Column name -> distinct count; applied by name to whatever the input outputs,
    /// so it stays right when DataFusion prunes or reorders the scan's columns.
    distinct: Arc<HashMap<String, usize>>,
    /// From [`equality_divisor`]: the input's row count is divided by this (1 = unchanged).
    rows_divisor: usize,
}

impl KeyStatisticsExec {
    /// Wrap `input`, or return it unchanged when it has nothing to adjust.
    fn wrap(
        input: Arc<dyn ExecutionPlan>,
        distinct: &HashMap<String, usize>,
        rows_divisor: usize,
    ) -> Arc<dyn ExecutionPlan> {
        let schema = input.schema();
        let relevant: HashMap<String, usize> = distinct
            .iter()
            .filter(|(name, _)| schema.field_with_name(name).is_ok())
            .map(|(name, n)| (name.clone(), *n))
            .collect();
        if relevant.is_empty() && rows_divisor == 1 {
            return input;
        }
        Arc::new(Self {
            input,
            distinct: Arc::new(relevant),
            rows_divisor,
        })
    }
}

impl DisplayAs for KeyStatisticsExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        let mut columns: Vec<_> = self.distinct.iter().collect();
        columns.sort();
        let shown: Vec<String> = columns.iter().map(|(c, n)| format!("{c}={n}")).collect();
        write!(f, "KeyStatisticsExec: distinct=[{}]", shown.join(", "))?;
        if self.rows_divisor > 1 {
            write!(f, ", rows/{}", self.rows_divisor)?;
        }
        Ok(())
    }
}

impl ExecutionPlan for KeyStatisticsExec {
    fn name(&self) -> &str {
        "KeyStatisticsExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Self::wrap(
            children.swap_remove(0),
            &self.distinct,
            self.rows_divisor,
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.input.execute(partition, context)
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        let mut stats = Statistics::clone(self.input.partition_statistics(partition)?.as_ref());
        if self.rows_divisor > 1 {
            if let Some(&n) = stats.num_rows.get_value() {
                stats.num_rows = Precision::Inexact(n.div_ceil(self.rows_divisor));
                stats.total_byte_size = stats.total_byte_size.to_inexact();
            }
        }
        let schema = self.input.schema();
        for (field, column) in schema
            .fields()
            .iter()
            .zip(stats.column_statistics.iter_mut())
        {
            if let Some(&n) = self.distinct.get(field.name()) {
                // Inexact: pushed-down filters may remove values. DataFusion also caps a
                // distinct count at the (estimated) row count.
                column.distinct_count = Precision::Inexact(n);
            }
        }
        Ok(Arc::new(stats))
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterDescription> {
        FilterDescription::from_children(parent_filters, &self.children())
    }

    fn handle_child_pushdown_result(
        &self,
        _phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        Ok(FilterPushdownPropagation::if_all(child_pushdown_result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn scan_reports_distinct_counts_for_named_columns_only() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("src", DataType::Int64, false),
            Field::new("dst", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 1, 2, 3])),
                Arc::new(Int64Array::from(vec![7, 8, 7, 7])),
            ],
        )
        .unwrap();
        let inner = Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap());
        let table = KeyStatisticsProvider::new(inner, HashMap::from([("dst".to_string(), 2)]));
        let ctx = SessionContext::new();
        let plan = table.scan(&ctx.state(), None, &[], None).await.unwrap();
        let stats = plan.partition_statistics(None).unwrap();
        assert_eq!(
            stats.column_statistics[1].distinct_count,
            Precision::Inexact(2)
        );
        assert_eq!(stats.column_statistics[0].distinct_count, Precision::Absent);

        // Projecting the counted column away leaves the inner plan unwrapped.
        let plan = table
            .scan(&ctx.state(), Some(&vec![0]), &[], None)
            .await
            .unwrap();
        assert_ne!(plan.name(), "KeyStatisticsExec");

        // A pushed equality filter on a counted column divides the row estimate;
        // other filters leave it alone.
        use datafusion::prelude::{col, lit};
        let filters = [col("dst").eq(lit(7_i64)), col("src").gt(lit(1_i64))];
        let plan = table
            .scan(&ctx.state(), None, &filters, None)
            .await
            .unwrap();
        let stats = plan.partition_statistics(None).unwrap();
        assert_eq!(stats.num_rows, Precision::Inexact(2));
    }
}
