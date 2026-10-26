// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The lance-csr Authors

//! Strict Cypher numeric arguments and rounding over DataFusion's math kernels.

use std::sync::Arc;

use arrow::array::{AsArray, Float64Array};
use arrow::compute::{cast_with_options, CastOptions};
use arrow_schema::{DataType, Field};
use datafusion::common::plan_err;
use datafusion::error::Result;
use datafusion::functions::math;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherMath {
    name: String,
    signature: Signature,
}

impl ScalarUDFImpl for CypherMath {
    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        let input = &types[0];
        if !input.is_integer() && !input.is_floating() && !input.is_null() {
            return plan_err!("{} does not accept {input} for argument 1", self.name);
        }
        Ok(if self.name == "abs" && input.is_integer() {
            DataType::Int64
        } else {
            DataType::Float64
        })
    }

    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let output_type = args.return_type().clone();
        let input = cast_with_options(
            &args.args[0].to_array(args.number_rows)?,
            &output_type,
            &CastOptions {
                safe: false,
                ..Default::default()
            },
        )?;
        if self.name == "round" {
            let rounded = input
                .as_primitive::<arrow::datatypes::Float64Type>()
                .iter()
                .map(|n| {
                    n.map(|n| {
                        // Cypher rounds ties toward positive infinity and returns positive zero.
                        let rounded = if n.fract() == -0.5 {
                            n.ceil()
                        } else {
                            n.round()
                        };
                        if rounded == 0.0 {
                            0.0
                        } else {
                            rounded
                        }
                    })
                })
                .collect::<Float64Array>();
            return Ok(ColumnarValue::Array(Arc::new(rounded)));
        }
        let udf = match self.name.as_str() {
            "abs" => math::abs(),
            "floor" => math::floor(),
            "ceil" => math::ceil(),
            "sqrt" => math::sqrt(),
            _ => unreachable!("validated math function"),
        };
        args.args = vec![ColumnarValue::Array(input)];
        args.arg_fields = vec![Arc::new(Field::new("arg", output_type, true))];
        udf.invoke_with_args(args)
    }
}

pub(super) fn call(name: &str, args: Vec<Expr>) -> Expr {
    ScalarUDF::new_from_impl(CypherMath {
        name: name.to_lowercase(),
        signature: Signature::any(1, Volatility::Immutable),
    })
    .call(args)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::GraphConfig, query::CypherQuery};
    use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch};
    use std::collections::HashMap;

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
    async fn cypher_math_rounding_and_overflow() {
        for (expression, expected) in [
            ("abs(-1.5)", 1.5),
            ("round(-1.5)", -1.0),
            ("round(1.5)", 2.0),
            ("round(4503599627370495.5)", 4503599627370496.0),
            ("round(4503599627370497.0)", 4503599627370497.0),
            ("floor(-1.5)", -2.0),
            ("ceil(-1.5)", -1.0),
            ("sqrt(9)", 3.0),
        ] {
            let out = run(expression).await.unwrap();
            assert_eq!(
                out.column(0)
                    .as_primitive::<arrow::datatypes::Float64Type>()
                    .value(0),
                expected,
                "{expression}"
            );
        }
        let out = run("abs(-5)").await.unwrap();
        assert_eq!(
            out.column(0)
                .as_primitive::<arrow::datatypes::Int64Type>()
                .value(0),
            5
        );
        assert!(run("abs(-9223372036854775807 - 1)").await.is_err());
    }

    #[tokio::test]
    async fn cypher_math_validates_types_arity_and_nulls() {
        for name in ["abs", "round", "floor", "ceil", "sqrt"] {
            let out = run(&format!("{name}(null)")).await.unwrap();
            assert_eq!(out.column(0).null_count(), 1, "{name}");
            for arg in ["'1'", "true", "[1]", "1, 2"] {
                assert!(
                    run(&format!("{name}({arg})")).await.is_err(),
                    "{name}({arg})"
                );
            }
        }
    }
}
