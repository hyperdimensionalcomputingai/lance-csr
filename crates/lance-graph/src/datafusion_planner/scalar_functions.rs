// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The lance-csr Authors

//! Cypher scalar semantics: strict argument kinds, Unicode characters, and nulls.
//! List operations reuse DataFusion's Arrow kernels; string slicing differs from SQL.

use std::sync::Arc;

use arrow::array::{
    new_null_array, Array, AsArray, Int64Array, ListBuilder, StringArray, StringBuilder,
};
use arrow::compute::{cast, cast_with_options, CastOptions};
use arrow_schema::{DataType, Field};
use datafusion::common::{exec_err, plan_err, ScalarValue};
use datafusion::error::Result;
use datafusion::functions_nested::{extract, length, range, reverse};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherScalar {
    name: String,
    signature: Signature,
}

fn is_string(t: &DataType) -> bool {
    matches!(t, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View)
}

fn list_field(t: &DataType) -> Option<&Arc<Field>> {
    match t {
        DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => Some(f),
        _ => None,
    }
}

impl ScalarUDFImpl for CypherScalar {
    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        for (i, t) in types.iter().enumerate() {
            let valid = match self.name.as_str() {
                "range" => t.is_integer(),
                "__predicate_all" | "__predicate_any" | "__predicate_none"
                | "__predicate_single" => list_field(t)
                    .is_some_and(|f| matches!(f.data_type(), DataType::Boolean | DataType::Null)),
                "keys" | "__entity_keys" => matches!(t, DataType::Struct(_)),
                "labels" => false,
                "size" | "reverse" => is_string(t) || list_field(t).is_some(),
                "last" | "tail" => list_field(t).is_some(),
                "left" | "right" | "substring" if i > 0 => t.is_integer(),
                _ => is_string(t),
            };
            if !valid && !t.is_null() {
                return plan_err!("{} does not accept {t} for argument {}", self.name, i + 1);
            }
        }
        Ok(match self.name.as_str() {
            "__predicate_all" | "__predicate_any" | "__predicate_none" | "__predicate_single" => {
                DataType::Boolean
            }
            "size" => DataType::Int64,
            "range" => DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            "split" | "keys" | "__entity_keys" | "labels" => {
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
            }
            "last" => list_field(&types[0])
                .map(|f| f.data_type().clone())
                .unwrap_or(DataType::Null),
            "tail" => match &types[0] {
                DataType::FixedSizeList(f, _) => DataType::List(f.clone()),
                t => t.clone(),
            },
            "reverse" if list_field(&types[0]).is_some() => types[0].clone(),
            _ => DataType::Utf8,
        })
    }

    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let output_type = args.return_type().clone();
        if args.args[0].data_type().is_null() && self.name != "range" {
            return Ok(ColumnarValue::Array(new_null_array(
                &output_type,
                args.number_rows,
            )));
        }
        if let Some(kind) = self.name.strip_prefix("__predicate_") {
            let input = args.args[0].to_array(args.number_rows)?;
            let mut result = arrow::array::BooleanBuilder::with_capacity(input.len());
            for row in 0..input.len() {
                if input.is_null(row) {
                    result.append_null();
                    continue;
                }
                let values = match input.data_type() {
                    DataType::List(_) => input.as_list::<i32>().value(row),
                    DataType::LargeList(_) => input.as_list::<i64>().value(row),
                    DataType::FixedSizeList(_, _) => input.as_fixed_size_list().value(row),
                    _ => unreachable!("validated list predicate input"),
                };
                let values = cast(&values, &DataType::Boolean)?;
                result.append_option(super::list_folds::predicate_result(
                    kind,
                    values.as_boolean(),
                    0,
                    values.len(),
                ));
            }
            return Ok(ColumnarValue::Array(Arc::new(result.finish())));
        }
        if matches!(self.name.as_str(), "keys" | "__entity_keys") {
            let input = args.args[0].to_array(args.number_rows)?;
            let map = input.as_struct();
            let nulls: Vec<_> = map
                .columns()
                .iter()
                .map(|column| column.logical_nulls())
                .collect();
            let entity = self.name == "__entity_keys";
            let mut result = ListBuilder::new(StringBuilder::new());
            for row in 0..input.len() {
                let missing = |i: usize| nulls[i].as_ref().is_some_and(|nulls| nulls.is_null(row));
                if input.is_null(row) || (entity && (0..map.num_columns()).all(missing)) {
                    result.append(false);
                    continue;
                }
                for (i, field) in map.fields().iter().enumerate() {
                    if !entity || !missing(i) {
                        result.values().append_value(field.name());
                    }
                }
                result.append(true);
            }
            return Ok(ColumnarValue::Array(Arc::new(result.finish())));
        }
        if self.name == "range" {
            for arg in &mut args.args {
                *arg = ColumnarValue::Array(cast_with_options(
                    &arg.to_array(args.number_rows)?,
                    &DataType::Int64,
                    &CastOptions {
                        safe: false,
                        ..Default::default()
                    },
                )?);
            }
            return range::gen_series_udf().invoke_with_args(args);
        }
        if list_field(&args.args[0].data_type()).is_some() {
            let udf = match self.name.as_str() {
                "size" => length::array_length_udf(),
                "reverse" => reverse::array_reverse_udf(),
                "last" => {
                    if let DataType::FixedSizeList(field, _) = args.args[0].data_type() {
                        args.args[0] = ColumnarValue::Array(cast(
                            &args.args[0].to_array(args.number_rows)?,
                            &DataType::List(field),
                        )?);
                    }
                    args.args
                        .push(ColumnarValue::Scalar(ScalarValue::Int64(Some(-1))));
                    extract::array_element_udf()
                }
                "tail" => {
                    args.args[0] = ColumnarValue::Array(cast(
                        &args.args[0].to_array(args.number_rows)?,
                        &output_type,
                    )?);
                    args.args.extend(
                        [2, -1].map(|n| ColumnarValue::Scalar(ScalarValue::Int64(Some(n)))),
                    );
                    extract::array_slice_udf()
                }
                _ => unreachable!("validated list function"),
            };
            let rows = args.number_rows;
            args.arg_fields = args
                .args
                .iter()
                .map(|arg| Arc::new(Field::new("arg", arg.data_type(), true)))
                .collect();
            args.return_field = Arc::new(Field::new(
                udf.name(),
                udf.return_type(
                    &args
                        .args
                        .iter()
                        .map(ColumnarValue::data_type)
                        .collect::<Vec<_>>(),
                )?,
                true,
            ));
            let result = udf.invoke_with_args(args)?;
            return if self.name == "size" {
                Ok(ColumnarValue::Array(cast(
                    &result.to_array(rows)?,
                    &DataType::Int64,
                )?))
            } else {
                Ok(result)
            };
        }
        let arrays = args
            .args
            .iter()
            .enumerate()
            .map(|(i, arg)| {
                let t = if i > 0 && matches!(self.name.as_str(), "left" | "right" | "substring") {
                    DataType::Int64
                } else {
                    DataType::Utf8
                };
                cast(&arg.to_array(args.number_rows)?, &t)
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let strings = arrays[0].as_string::<i32>();
        if self.name == "size" {
            return Ok(ColumnarValue::Array(Arc::new(
                strings
                    .iter()
                    .map(|s| s.map(|s| s.chars().count() as i64))
                    .collect::<Int64Array>(),
            )));
        }
        if self.name == "split" {
            let delimiters = arrays[1].as_string::<i32>();
            let mut result = ListBuilder::new(StringBuilder::new());
            for (s, delimiter) in strings.iter().zip(delimiters.iter()) {
                match (s, delimiter) {
                    (Some(s), Some(delimiter)) => {
                        if s.is_empty() {
                            result.values().append_value("");
                        } else if delimiter.is_empty() {
                            for c in s.chars() {
                                result.values().append_value(c.to_string());
                            }
                        } else {
                            for part in s.split(delimiter) {
                                result.values().append_value(part);
                            }
                        }
                        result.append(true);
                    }
                    _ => result.append(false),
                }
            }
            return Ok(ColumnarValue::Array(Arc::new(result.finish())));
        }
        let result = strings
            .iter()
            .enumerate()
            .map(|(row, s)| {
                let Some(s) = s else { return Ok(None) };
                let value = match self.name.as_str() {
                    "trim" => s.trim().to_string(),
                    "ltrim" => s.trim_start().to_string(),
                    "rtrim" => s.trim_end().to_string(),
                    "reverse" => s.chars().rev().collect(),
                    "replace" => {
                        if arrays[1].is_null(row) || arrays[2].is_null(row) {
                            return Ok(None);
                        }
                        s.replace(
                            arrays[1].as_string::<i32>().value(row),
                            arrays[2].as_string::<i32>().value(row),
                        )
                    }
                    "left" | "right" | "substring" => {
                        let index = |i: usize| -> Result<usize> {
                            if arrays[i].is_null(row) {
                                return exec_err!(
                                    "{} requires a non-null integer for argument {}",
                                    self.name,
                                    i + 1
                                );
                            }
                            let n = arrays[i]
                                .as_primitive::<arrow::datatypes::Int64Type>()
                                .value(row);
                            if n < 0 {
                                return exec_err!(
                                    "{} requires a non-negative integer for argument {}",
                                    self.name,
                                    i + 1
                                );
                            }
                            Ok(n as usize)
                        };
                        let n = index(1)?;
                        let (start, count) = match self.name.as_str() {
                            "left" => (0, n),
                            "right" => (s.chars().count().saturating_sub(n), n),
                            _ => (
                                n,
                                if arrays.len() == 3 {
                                    index(2)?
                                } else {
                                    usize::MAX
                                },
                            ),
                        };
                        s.chars().skip(start).take(count).collect()
                    }
                    _ => unreachable!("validated string function"),
                };
                Ok(Some(value))
            })
            .collect::<Result<StringArray>>()?;
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

pub(super) fn call(name: &str, args: Vec<Expr>) -> Expr {
    ScalarUDF::new_from_impl(CypherScalar {
        name: name.to_lowercase(),
        signature: Signature::any(args.len(), Volatility::Immutable),
    })
    .call(args)
}

/// SQL equivalents for the list operations supported before Cypher-specific wrappers.
pub(crate) fn sql_expr(
    function: &datafusion::logical_expr::expr::ScalarFunction,
    schema: &datafusion::common::DFSchema,
) -> Result<Option<Expr>> {
    use datafusion::logical_expr::{expr_fn::cast, ExprSchemable};

    if !function.func.inner().is::<CypherScalar>() {
        return Ok(None);
    }
    let args = &function.args;
    Ok(match function.name() {
        "size" if list_field(&args[0].get_type(schema)?).is_some() => Some(cast(
            length::array_length_udf().call(args.clone()),
            DataType::Int64,
        )),
        "range" => Some(
            range::gen_series_udf().call(
                args.iter()
                    .cloned()
                    .map(|arg| cast(arg, DataType::Int64))
                    .collect(),
            ),
        ),
        _ => None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::GraphConfig, query::CypherQuery};
    use arrow::array::{ArrayRef, RecordBatch};
    use std::collections::HashMap;

    #[tokio::test]
    async fn range_rejects_unsigned_overflow() {
        let batch = RecordBatch::try_from_iter(vec![
            ("id", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
            (
                "large",
                Arc::new(arrow::array::UInt64Array::from(vec![1_u64 << 63])) as ArrayRef,
            ),
        ])
        .unwrap();
        let config = GraphConfig::builder()
            .node_table("Person", "id")
            .build()
            .unwrap();
        for expression in [
            "range(p.large, 1)",
            "range(1, p.large)",
            "range(1, 2, p.large)",
        ] {
            let error = CypherQuery::new(&format!("MATCH (p:Person) RETURN {expression} AS value"))
                .unwrap()
                .with_config(config.clone())
                .execute(HashMap::from([("Person".to_string(), batch.clone())]), None)
                .await
                .unwrap_err();
            assert!(error.to_string().contains("cast"), "{expression}: {error}");
        }
    }

    async fn run(expression: &str) -> crate::error::Result<RecordBatch> {
        let batch =
            RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(vec![1])) as ArrayRef)])
                .unwrap();
        CypherQuery::new(&format!("RETURN {expression} AS value"))?
            .with_config(
                GraphConfig::builder()
                    .node_table("Person", "id")
                    .build()
                    .unwrap(),
            )
            .execute(HashMap::from([("Person".to_string(), batch)]), None)
            .await
    }

    #[tokio::test]
    async fn cypher_entity_map_keys() {
        for (expression, expected) in [
            ("keys({x: null, y: 1})", vec!["x", "y"]),
            ("keys({})", vec![]),
        ] {
            let out = run(expression).await.unwrap();
            let keys = out.column(0).as_list::<i32>().value(0);
            assert_eq!(
                keys.as_string::<i32>()
                    .iter()
                    .map(Option::unwrap)
                    .collect::<Vec<_>>(),
                expected
            );
        }
        for expression in ["keys(null)", "labels(null)"] {
            assert_eq!(
                run(expression)
                    .await
                    .unwrap()
                    .column(0)
                    .logical_null_count(),
                1
            );
        }
        for expression in ["keys(1)", "labels(1)"] {
            assert!(run(expression).await.is_err());
        }
    }

    #[tokio::test]
    async fn cypher_scalar_unicode_and_slicing() {
        for (expression, expected) in [
            ("left('a猫😀z', 3)", "a猫😀"),
            ("right('a猫😀z', 2)", "😀z"),
            ("substring('a猫😀z', 1, 2)", "猫😀"),
            ("substring('a猫😀z', 2)", "😀z"),
            ("substring('x', 99, 2)", ""),
            ("right('x', 0)", ""),
            ("left('x', 99)", "x"),
            ("reverse('a猫😀z')", "z😀猫a"),
            ("trim('  猫  ')", "猫"),
            ("lTrim('  猫  ')", "猫  "),
            ("rTrim('  猫  ')", "  猫"),
            ("replace('a猫a', 'a', '😀')", "😀猫😀"),
        ] {
            let out = run(expression).await.unwrap();
            assert_eq!(
                out.column(0).as_string::<i32>().value(0),
                expected,
                "{expression}"
            );
        }
        let out = run("size('a猫😀z')").await.unwrap();
        assert_eq!(
            out.column(0)
                .as_primitive::<arrow::datatypes::Int64Type>()
                .value(0),
            4
        );
    }

    #[tokio::test]
    async fn cypher_scalar_list_operations() {
        for (expression, expected) in [
            ("reverse([1, 2, 3])", vec![3, 2, 1]),
            ("tail([1, 2, 3])", vec![2, 3]),
            ("tail([1])", vec![]),
            ("range(5, 1, -2)", vec![5, 3, 1]),
            ("range(1, 5, -2)", vec![]),
            ("range(0, 4, 2)", vec![0, 2, 4]),
        ] {
            let out = run(expression).await.unwrap();
            let values = out.column(0).as_list::<i32>().value(0);
            let values: Vec<_> = values
                .as_primitive::<arrow::datatypes::Int64Type>()
                .values()
                .to_vec();
            assert_eq!(values, expected, "{expression}");
        }
        for (expression, expected) in [("last([1, 2, 3])", 3), ("size([[1, 2], [3]])", 2)] {
            let out = run(expression).await.unwrap();
            assert_eq!(
                out.column(0)
                    .as_primitive::<arrow::datatypes::Int64Type>()
                    .value(0),
                expected
            );
        }
        for (expression, expected) in [
            ("split('a,,b,', ',')", vec!["a", "", "b", ""]),
            ("split('猫😀', '')", vec!["猫", "😀"]),
            ("split('', ',')", vec![""]),
        ] {
            let out = run(expression).await.unwrap();
            let values = out.column(0).as_list::<i32>().value(0);
            assert_eq!(
                values
                    .as_string::<i32>()
                    .iter()
                    .map(Option::unwrap)
                    .collect::<Vec<_>>(),
                expected
            );
        }
    }

    #[tokio::test]
    async fn cypher_scalar_nulls_and_invalid_arguments() {
        for expression in [
            "size(null)",
            "reverse(null)",
            "last(null)",
            "tail(null)",
            "last([])",
            "left(null, null)",
            "substring(null, -1)",
            "replace('x', null, '')",
            "split('x', null)",
            "range(null, 3)",
        ] {
            let out = run(expression).await.unwrap();
            assert_eq!(out.column(0).logical_null_count(), 1, "{expression}");
        }
        for expression in [
            "left('x', -1)",
            "right('x', null)",
            "substring('x', -1)",
            "substring('x', 0, null)",
            "range(1, 3, 0)",
            "range(1.5, 3)",
            "size(1)",
            "reverse(1)",
            "tail('abc')",
            "last(2)",
            "left('x', 1.5)",
            "split('x', 1)",
            "substring('x')",
            "range(1)",
        ] {
            assert!(run(expression).await.is_err(), "{expression} should fail");
        }
    }
}
