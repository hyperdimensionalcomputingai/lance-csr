// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The lance-csr Authors

//! Cypher ordered folds and three-valued list predicates, using DataFusion lambdas.

use std::sync::Arc;

use arrow::array::{
    new_empty_array, new_null_array, Array, ArrayRef, AsArray, BooleanArray, BooleanBuilder,
};
use arrow::compute::{cast, concat, take_arrays};
use arrow_schema::{DataType, Field, FieldRef};
use datafusion::common::utils::{adjust_offsets_for_slice, list_values, list_values_row_number};
use datafusion::common::{exec_err, plan_err, Result};
use datafusion::logical_expr::expr::{HigherOrderFunction, Lambda};
use datafusion::logical_expr::{
    ColumnarValue, Expr, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs, HigherOrderSignature,
    HigherOrderUDF, HigherOrderUDFImpl, LambdaParametersProgress, ValueOrLambda, Volatility,
};

pub(super) fn reduce(list: Expr, initial: Expr, params: Vec<String>, body: Expr) -> Expr {
    Expr::HigherOrderFunction(HigherOrderFunction::new(
        Arc::new(HigherOrderUDF::from(CypherFold::new("reduce"))),
        vec![list, initial, Expr::Lambda(Lambda::new(params, body))],
    ))
}

pub(super) fn predicate(kind: &str, list: Expr, variable: String, body: Expr) -> Expr {
    Expr::HigherOrderFunction(HigherOrderFunction::new(
        Arc::new(HigherOrderUDF::from(CypherFold::new(kind))),
        vec![list, Expr::Lambda(Lambda::new(vec![variable], body))],
    ))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherFold {
    name: String,
    signature: HigherOrderSignature,
}

impl CypherFold {
    fn new(name: &str) -> Self {
        let mut arguments = vec![ValueOrLambda::Value(())];
        if name == "reduce" {
            arguments.push(ValueOrLambda::Value(()));
        }
        arguments.push(ValueOrLambda::Lambda(()));
        Self {
            name: name.to_string(),
            signature: HigherOrderSignature::exact(arguments, Volatility::Immutable),
        }
    }
}

fn list_type(data_type: &DataType) -> Result<DataType> {
    match data_type {
        DataType::List(_) | DataType::LargeList(_) => Ok(data_type.clone()),
        DataType::ListView(f) | DataType::FixedSizeList(f, _) => Ok(DataType::List(f.clone())),
        DataType::LargeListView(f) => Ok(DataType::LargeList(f.clone())),
        DataType::Null => Ok(DataType::new_list(DataType::Null, true)),
        other => plan_err!("reduce/list predicate requires a list, got {other}"),
    }
}

impl HigherOrderUDFImpl for CypherFold {
    fn name(&self) -> &str {
        &self.name
    }
    fn signature(&self) -> &HigherOrderSignature {
        &self.signature
    }

    fn coerce_value_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        let mut result = types.to_vec();
        result[0] = list_type(&types[0])?;
        Ok(result)
    }

    fn lambda_parameters(
        &self,
        step: usize,
        fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
    ) -> Result<LambdaParametersProgress> {
        let ValueOrLambda::Value(list) = &fields[0] else {
            return plan_err!("expected list value");
        };
        let normalized = list_type(list.data_type())?;
        let (DataType::List(element) | DataType::LargeList(element)) = normalized else {
            unreachable!()
        };
        if self.name != "reduce" {
            return Ok(LambdaParametersProgress::Complete(vec![vec![element]]));
        }
        let ValueOrLambda::Value(initial) = &fields[1] else {
            return plan_err!("expected initial value");
        };
        let ValueOrLambda::Lambda(body) = &fields[2] else {
            return plan_err!("expected reduce body");
        };
        match body {
            None => Ok(LambdaParametersProgress::Partial(vec![Some(vec![
                initial.clone(),
                element,
            ])])),
            Some(body) => {
                // One rebinding permits integer-to-float promotion and a null initializer.
                if step > 2 {
                    return plan_err!("reduce accumulator type does not converge");
                }
                Ok(LambdaParametersProgress::Complete(vec![vec![
                    body.clone(),
                    element,
                ]]))
            }
        }
    }

    fn coerce_values_for_lambdas(
        &self,
        fields: &[ValueOrLambda<DataType, DataType>],
    ) -> Result<Option<Vec<DataType>>> {
        if self.name != "reduce" {
            return Ok(None);
        }
        let [ValueOrLambda::Value(list), ValueOrLambda::Value(initial), ValueOrLambda::Lambda(body)] =
            fields
        else {
            return plan_err!("invalid reduce arguments");
        };
        if initial != body
            && *initial != DataType::Null
            && !(initial.is_numeric() && body.is_numeric())
        {
            return plan_err!("reduce initial value ({initial}) and accumulator ({body}) must have compatible types");
        }
        Ok(Some(vec![list.clone(), body.clone()]))
    }

    fn return_field_from_args(&self, args: HigherOrderReturnFieldArgs) -> Result<FieldRef> {
        let Some(ValueOrLambda::Lambda(body)) = args.arg_fields.last() else {
            return plan_err!("expected lambda body");
        };
        if self.name == "reduce" {
            return Ok(Arc::new(body.as_ref().clone().with_nullable(true)));
        }
        if !matches!(body.data_type(), DataType::Boolean | DataType::Null) {
            return plan_err!(
                "{} predicate must return boolean, got {}",
                self.name,
                body.data_type()
            );
        }
        Ok(Arc::new(Field::new("", DataType::Boolean, true)))
    }

    fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> Result<ColumnarValue> {
        let ValueOrLambda::Value(list) = &args.args[0] else {
            return exec_err!("expected list value");
        };
        let Some(ValueOrLambda::Lambda(lambda)) = args.args.last() else {
            return exec_err!("expected lambda body");
        };
        let list = list.to_array(args.number_rows)?;
        let ranges: Vec<(usize, usize)> = match list.data_type() {
            DataType::List(_) => list
                .as_list::<i32>()
                .value_offsets()
                .windows(2)
                .map(|w| (w[0] as usize, w[1] as usize))
                .collect(),
            DataType::LargeList(_) => list
                .as_list::<i64>()
                .value_offsets()
                .windows(2)
                .map(|w| (w[0] as usize, w[1] as usize))
                .collect(),
            other => return exec_err!("expected list, got {other}"),
        };
        if self.name == "reduce" {
            let ValueOrLambda::Value(initial) = &args.args[1] else {
                return exec_err!("expected initial value");
            };
            let initial = cast(&initial.to_array(args.number_rows)?, args.return_type())?;
            let values = match list.data_type() {
                DataType::List(_) => list.as_list::<i32>().values(),
                DataType::LargeList(_) => list.as_list::<i64>().values(),
                _ => unreachable!(),
            };
            let mut output: Vec<ArrayRef> = Vec::with_capacity(list.len());
            for (row, (start, end)) in ranges.into_iter().enumerate() {
                if list.is_null(row) {
                    output.push(new_null_array(args.return_type(), 1));
                    continue;
                }
                let mut accumulator = initial.slice(row, 1);
                // Each step sees the previous step's result, in list order.
                for index in start..end {
                    let state = || Ok(accumulator.clone());
                    let element = || Ok(values.slice(index, 1));
                    accumulator = lambda
                        .evaluate(&[&state, &element], |captures| {
                            Ok(captures.iter().map(|a| a.slice(row, 1)).collect())
                        })?
                        .into_array(1)?;
                    if accumulator.data_type() != args.return_type() {
                        return exec_err!(
                            "reduce accumulator type changed from {} to {}",
                            args.return_type(),
                            accumulator.data_type()
                        );
                    }
                }
                output.push(accumulator);
            }
            let result = if output.is_empty() {
                new_empty_array(args.return_type())
            } else {
                concat(&output.iter().map(|a| a.as_ref()).collect::<Vec<_>>())?
            };
            return Ok(ColumnarValue::Array(result));
        }
        let values = list_values(&list)?;
        let predicate = if values.is_empty() {
            new_empty_array(&DataType::Boolean)
        } else {
            let parameter = || Ok(values.clone());
            cast(
                &lambda
                    .evaluate(&[&parameter], |captures| {
                        Ok(take_arrays(
                            captures,
                            &list_values_row_number(&list)?,
                            None,
                        )?)
                    })?
                    .into_array(values.len())?,
                &DataType::Boolean,
            )?
        };
        let predicate = predicate.as_boolean();
        let ranges: Vec<(usize, usize)> = match list.data_type() {
            DataType::List(_) => adjust_offsets_for_slice(list.as_list::<i32>())
                .windows(2)
                .map(|w| (w[0] as usize, w[1] as usize))
                .collect(),
            DataType::LargeList(_) => adjust_offsets_for_slice(list.as_list::<i64>())
                .windows(2)
                .map(|w| (w[0] as usize, w[1] as usize))
                .collect(),
            _ => unreachable!(),
        };
        let mut output = BooleanBuilder::with_capacity(list.len());
        for (row, (start, end)) in ranges.into_iter().enumerate() {
            output.append_option(if list.is_null(row) {
                None
            } else {
                predicate_result(&self.name, predicate, start, end)
            });
        }
        Ok(ColumnarValue::Array(Arc::new(output.finish())))
    }
}

pub(super) fn predicate_result(
    kind: &str,
    values: &BooleanArray,
    start: usize,
    end: usize,
) -> Option<bool> {
    let mut trues = 0;
    let mut falses = 0;
    let mut nulls = 0;
    for i in start..end {
        if values.is_null(i) {
            nulls += 1;
        } else if values.value(i) {
            trues += 1;
        } else {
            falses += 1;
        }
    }
    match kind {
        "all" if falses > 0 => Some(false),
        "any" if trues > 0 => Some(true),
        "none" if trues > 0 => Some(false),
        "single" if trues > 1 => Some(false),
        _ if nulls > 0 => None,
        "all" | "none" => Some(true),
        "any" => Some(false),
        "single" => Some(trues == 1),
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_predicates_use_three_valued_logic() {
        for (values, expected) in [
            (vec![], [Some(true), Some(false), Some(true), Some(false)]),
            (vec![None], [None, None, None, None]),
            (
                vec![Some(true), None],
                [None, Some(true), Some(false), None],
            ),
            (vec![Some(false), None], [Some(false), None, None, None]),
            (
                vec![Some(true), Some(true), None],
                [None, Some(true), Some(false), Some(false)],
            ),
            (
                vec![Some(true), Some(false)],
                [Some(false), Some(true), Some(false), Some(true)],
            ),
        ] {
            let values = BooleanArray::from(values);
            for (kind, want) in ["all", "any", "none", "single"].iter().zip(expected) {
                assert_eq!(
                    predicate_result(kind, &values, 0, values.len()),
                    want,
                    "{kind}: {values:?}"
                );
            }
        }
    }
}
