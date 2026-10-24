// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The lance-csr Authors

//! Scalar functions with Cypher semantics that DataFusion's built-ins don't match.

use std::sync::{Arc, LazyLock};

use arrow_schema::DataType;
use datafusion::error::Result;
use datafusion::functions_nested::make_array::make_array_udf;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

/// A list literal `[a, b, ...]`. Like `make_array`, except that elements of different
/// kinds become strings instead of failing to cast: Cypher lists may mix types (LDBC IC1's
/// `[uni.name, studyAt.classYear, uniCity.name]`), Arrow lists hold one type.
#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherList {
    signature: Signature,
}

impl ScalarUDFImpl for CypherList {
    fn name(&self) -> &str {
        "cypher_list"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        make_array_udf().return_type(arg_types)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let kinds: Vec<&DataType> = arg_types.iter().filter(|t| !t.is_null()).collect();
        let text =
            |t: &&DataType| matches!(t, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View);
        if kinds.windows(2).any(|w| w[0] != w[1]) && kinds.iter().any(text) {
            Ok(vec![DataType::Utf8; arg_types.len()])
        } else {
            make_array_udf().coerce_types(arg_types)
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_array_udf().invoke_with_args(args)
    }
}

static CYPHER_LIST: LazyLock<Arc<ScalarUDF>> = LazyLock::new(|| {
    Arc::new(ScalarUDF::new_from_impl(CypherList {
        signature: Signature::user_defined(Volatility::Immutable),
    }))
});

/// `[items...]` as a Cypher list.
pub(crate) fn list(items: Vec<Expr>) -> Expr {
    if items.is_empty() {
        return datafusion::functions_nested::expr_fn::make_array(items);
    }
    CYPHER_LIST.call(items)
}
