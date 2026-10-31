// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! A node's columns fetched by id through its node index, for the rows of `input`.
//!
//! The planner groups `WITH b, ...` by `b`'s id alone when `b`'s label has a node index
//! (unique ids), then adds `b`'s other columns with this node: one lookup per group
//! instead of carrying them through the aggregate for every input row. Unlike a join
//! back to the node table, the cost follows the rows looked up, not the table's size.

use super::*;

/// `input` plus the columns of the node whose id is in `key_column`. A null or unknown
/// id (an unmatched OPTIONAL MATCH) gets nulls.
#[derive(Debug, Clone)]
pub struct NodeLookupNode {
    input: LogicalPlan,
    key_column: String,
    variable: String,
    rows: Arc<NodeRows>,
    /// Output fields (`b__name`), nullable.
    fields: Vec<Field>,
    /// For each output field, its column index in `rows` (ascending).
    columns: Vec<usize>,
    schema: DFSchemaRef,
}

impl NodeLookupNode {
    /// `wanted` pairs each output name (`b__name`) with the node table's column name.
    pub fn try_new(
        input: LogicalPlan,
        key_column: String,
        variable: String,
        rows: Arc<NodeRows>,
        wanted: &[(String, String)],
    ) -> Result<Self> {
        let mut columns: Vec<(usize, String)> = wanted
            .iter()
            .map(|(name, column)| {
                rows.schema
                    .index_of(column)
                    .map(|c| (c, name.clone()))
                    .map_err(|e| plan_error(format!("NodeLookup column: {}", e)))
            })
            .collect::<Result<_>>()?;
        columns.sort_unstable();
        let fields = columns
            .iter()
            .map(|(c, name)| Field::new(name, rows.schema.field(*c).data_type().clone(), true))
            .collect();
        Self::with_fields(
            input,
            key_column,
            variable,
            rows,
            fields,
            columns.into_iter().map(|(c, _)| c).collect(),
        )
    }

    fn with_fields(
        input: LogicalPlan,
        key_column: String,
        variable: String,
        rows: Arc<NodeRows>,
        fields: Vec<Field>,
        columns: Vec<usize>,
    ) -> Result<Self> {
        let added = DFSchema::try_from(Schema::new(fields.clone()))
            .map_err(|e| plan_error(format!("NodeLookup schema: {}", e)))?;
        let schema = input
            .schema()
            .join(&added)
            .map_err(|e| plan_error(format!("NodeLookup schema: {}", e)))?;
        Ok(Self {
            input,
            key_column,
            variable,
            rows,
            fields,
            columns,
            schema: Arc::new(schema),
        })
    }

    /// The same lookup with only the fetched columns in `used`; `Some(input)` when none
    /// remain, so the lookup can be dropped. `None` when every column is used.
    pub(super) fn pruned(
        &self,
        used: &HashSet<String>,
    ) -> Option<std::result::Result<Self, LogicalPlan>> {
        let keep: Vec<usize> = (0..self.fields.len())
            .filter(|&i| used.contains(self.fields[i].name()))
            .collect();
        if keep.len() == self.fields.len() {
            return None;
        }
        if keep.is_empty() {
            return Some(Err(self.input.clone()));
        }
        Self::with_fields(
            self.input.clone(),
            self.key_column.clone(),
            self.variable.clone(),
            self.rows.clone(),
            keep.iter().map(|&i| self.fields[i].clone()).collect(),
            keep.iter().map(|&i| self.columns[i]).collect(),
        )
        .ok()
        .map(Ok)
    }

    pub(super) fn key_column(&self) -> &str {
        &self.key_column
    }

    fn describe(&self) -> String {
        let names: Vec<&str> = self.fields.iter().map(|f| f.name().as_str()).collect();
        format!(
            "key={}, take={}[{}]",
            self.key_column,
            self.variable,
            names.join(", ")
        )
    }
}

// Logical nodes must be comparable; the node index compares by identity.
impl PartialEq for NodeLookupNode {
    fn eq(&self, other: &Self) -> bool {
        self.input == other.input
            && self.key_column == other.key_column
            && self.variable == other.variable
            && self.columns == other.columns
            && Arc::ptr_eq(&self.rows, &other.rows)
    }
}
impl Eq for NodeLookupNode {}
impl Hash for NodeLookupNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.input.hash(state);
        self.key_column.hash(state);
        self.variable.hash(state);
        self.columns.hash(state);
    }
}
impl PartialOrd for NodeLookupNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        (&self.key_column, &self.variable).partial_cmp(&(&other.key_column, &other.variable))
    }
}

impl UserDefinedLogicalNodeCore for NodeLookupNode {
    fn name(&self) -> &str {
        "NodeLookup"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    /// Filters on the fetched columns must stay above the lookup.
    fn prevent_predicate_push_down_columns(&self) -> HashSet<String> {
        self.fields.iter().map(|f| f.name().clone()).collect()
    }

    /// The input only needs the requested input columns plus the key.
    fn necessary_children_exprs(&self, output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        let input_width = self.input.schema().fields().len();
        let key = self
            .input
            .schema()
            .index_of_column_by_name(None, &self.key_column)?;
        let mut needed: Vec<usize> = output_columns
            .iter()
            .copied()
            .filter(|&i| i < input_width)
            .chain(std::iter::once(key))
            .collect();
        needed.sort_unstable();
        needed.dedup();
        Some(vec![needed])
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "NodeLookup: {}", self.describe())
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion::common::Result<Self> {
        let input = inputs.into_iter().next().expect("NodeLookup has one input");
        Self::with_fields(
            input,
            self.key_column.clone(),
            self.variable.clone(),
            self.rows.clone(),
            self.fields.clone(),
            self.columns.clone(),
        )
        .map_err(|e| datafusion::common::DataFusionError::Plan(e.to_string()))
    }
}

/// Executes [`NodeLookupNode`]: per batch, the key's node rows, then one take of the
/// wanted columns.
// shortcut: no read-once switch like `CsrExpandExec`'s targets, as the planner only looks
// up in-memory node tables; add one if Lance node tables are looked up.
#[derive(Debug)]
pub struct NodeLookupExec {
    input: Arc<dyn ExecutionPlan>,
    key_column: String,
    variable: String,
    rows: Arc<NodeRows>,
    columns: Vec<usize>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl NodeLookupExec {
    fn new(node: &NodeLookupNode, input: Arc<dyn ExecutionPlan>) -> Self {
        let schema: SchemaRef = Arc::new(node.schema.as_arrow().clone());
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            input.output_partitioning().clone(),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            input,
            key_column: node.key_column.clone(),
            variable: node.variable.clone(),
            rows: node.rows.clone(),
            columns: node.columns.clone(),
            schema,
            properties,
        }
    }

    /// The batch's rows with their node's columns appended.
    async fn lookup(
        batch: RecordBatch,
        key_column: &str,
        rows: &NodeRows,
        columns: &[usize],
        schema: &SchemaRef,
    ) -> datafusion::common::Result<RecordBatch> {
        let external = |e: GraphError| datafusion::common::DataFusionError::External(Box::new(e));
        let ids = int64_column(&batch, key_column).map_err(external)?;
        let mut positions = Vec::with_capacity(ids.len());
        // Row -> its slot in `positions`, or None for a null or unknown id.
        let slots: Vec<Option<u32>> = (0..ids.len())
            .map(|i| {
                let position = ids
                    .is_valid(i)
                    .then(|| rows.position(ids.value(i)))
                    .flatten()?;
                positions.push(position);
                Some(positions.len() as u32 - 1)
            })
            .collect();
        let fetched = rows.take(&positions, columns).await.map_err(external)?;
        let fetched = if positions.len() == ids.len() {
            fetched
        } else {
            let slots = UInt32Array::from(slots);
            fetched
                .iter()
                .map(|array| take(array.as_ref(), &slots, None))
                .collect::<std::result::Result<_, _>>()?
        };
        let mut output = batch.columns().to_vec();
        output.extend(fetched);
        Ok(RecordBatch::try_new(schema.clone(), output)?)
    }
}

impl DisplayAs for NodeLookupExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        let names: Vec<&str> = self.schema.fields()
            [self.schema.fields().len() - self.columns.len()..]
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        write!(
            f,
            "NodeLookupExec: key={}, take={}[{}]",
            self.key_column,
            self.variable,
            names.join(", ")
        )
    }
}

impl ExecutionPlan for NodeLookupExec {
    fn name(&self) -> &str {
        "NodeLookupExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let input = children
            .into_iter()
            .next()
            .expect("NodeLookupExec has one child");
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(self.schema.clone()),
            input.output_partitioning().clone(),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Arc::new(Self {
            input,
            key_column: self.key_column.clone(),
            variable: self.variable.clone(),
            rows: self.rows.clone(),
            columns: self.columns.clone(),
            schema: self.schema.clone(),
            properties,
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let (rows, columns, schema) =
            (self.rows.clone(), self.columns.clone(), self.schema.clone());
        let key_column = self.key_column.clone();
        let stream = input.then(move |batch| {
            let (rows, columns, schema) = (rows.clone(), columns.clone(), schema.clone());
            let key_column = key_column.clone();
            async move { Self::lookup(batch?, &key_column, &rows, &columns, &schema).await }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }
}

pub(super) fn plan_node_lookup(
    node: &NodeLookupNode,
    physical_inputs: &[Arc<dyn ExecutionPlan>],
) -> Arc<dyn ExecutionPlan> {
    Arc::new(NodeLookupExec::new(node, physical_inputs[0].clone()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;

    #[tokio::test]
    async fn looks_up_columns_with_nulls_for_unknown_ids() {
        let people = RecordBatch::try_from_iter([
            (
                "id",
                Arc::new(Int64Array::from(vec![10, 20, 30])) as ArrayRef,
            ),
            (
                "name",
                Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef,
            ),
        ])
        .unwrap();
        let rows = NodeRows::from_batch(people, "id").unwrap().unwrap();
        let input = RecordBatch::try_from_iter([(
            "b__id",
            Arc::new(Int64Array::from(vec![Some(30), None, Some(99), Some(10)])) as ArrayRef,
        )])
        .unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("b__id", DataType::Int64, true),
            Field::new("b__name", DataType::Utf8, true),
        ]));
        let out = NodeLookupExec::lookup(input, "b__id", &rows, &[1], &schema)
            .await
            .unwrap();
        let names = out.column(1).as_string::<i32>();
        let names: Vec<Option<&str>> = (0..names.len())
            .map(|i| names.is_valid(i).then(|| names.value(i)))
            .collect();
        assert_eq!(names, vec![Some("c"), None, None, Some("a")]);
    }
}
