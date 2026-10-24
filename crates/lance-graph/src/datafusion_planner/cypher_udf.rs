// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The lance-csr Authors

//! Scalar functions with Cypher semantics that DataFusion's built-ins don't match.

use std::sync::{Arc, LazyLock};

use arrow::array::{Array, AsArray};
use arrow::compute::{cast, date_part, DatePart};
use arrow_schema::{DataType, Field, FieldRef};
use datafusion::common::{exec_err, plan_err, ScalarValue};
use datafusion::error::Result;
use datafusion::functions_nested::make_array::make_array_udf;
use datafusion::logical_expr::{
    lit, ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, Volatility,
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

/// `value.field` on a value: a map (struct) key, matched case-insensitively like
/// properties, or a component of a date or timestamp (`birthday.month`, `t.year`).
#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherField {
    signature: Signature,
}

/// The date or time component `name` stands for, if any.
fn temporal_part(name: &str) -> Option<DatePart> {
    Some(match name.to_lowercase().as_str() {
        "year" => DatePart::Year,
        "quarter" => DatePart::Quarter,
        "month" => DatePart::Month,
        "week" => DatePart::Week,
        "day" => DatePart::Day,
        "dayofyear" | "ordinalday" => DatePart::DayOfYear,
        "hour" => DatePart::Hour,
        "minute" => DatePart::Minute,
        "second" => DatePart::Second,
        "millisecond" => DatePart::Millisecond,
        "microsecond" => DatePart::Microsecond,
        "nanosecond" => DatePart::Nanosecond,
        _ => return None,
    })
}

fn is_temporal(t: &DataType) -> bool {
    matches!(
        t,
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(..)
    )
}

/// Index of the struct field named `name`, ignoring case.
fn struct_field(fields: &arrow_schema::Fields, name: &str) -> Option<usize> {
    fields
        .iter()
        .position(|f| f.name().eq_ignore_ascii_case(name))
}

impl ScalarUDFImpl for CypherField {
    fn name(&self) -> &str {
        "cypher_field"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        unreachable!("return_field_from_args decides the type")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let Some(Some(ScalarValue::Utf8(Some(name)))) = args.scalar_arguments.get(1) else {
            return plan_err!("cypher_field takes a field name literal");
        };
        let data_type = match args.arg_fields[0].data_type() {
            DataType::Struct(fields) => match struct_field(fields, name) {
                Some(i) => fields[i].data_type().clone(),
                None => return plan_err!("no key '{name}' in this map"),
            },
            t if is_temporal(t) && temporal_part(name).is_some() => DataType::Int64,
            DataType::Null => DataType::Null,
            t => return plan_err!("can't read '.{name}' of a {t} value"),
        };
        Ok(Arc::new(Field::new(self.name(), data_type, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ColumnarValue::Scalar(ScalarValue::Utf8(Some(name))) = &args.args[1] else {
            return exec_err!("cypher_field takes a field name literal");
        };
        let base = args.args[0].to_array(args.number_rows)?;
        let result = match base.data_type() {
            DataType::Struct(fields) => {
                let i = struct_field(fields, name).expect("checked when planning");
                let column = base.as_struct().column(i).clone();
                // A null map gives null keys, whatever its child arrays hold.
                let nulls = arrow::buffer::NullBuffer::union(
                    base.logical_nulls().as_ref(),
                    column.logical_nulls().as_ref(),
                );
                let data = column.to_data().into_builder().nulls(nulls).build()?;
                arrow::array::make_array(data)
            }
            DataType::Null => base,
            _ => {
                let part = temporal_part(name).expect("checked when planning");
                cast(&date_part(&base, part)?, &DataType::Int64)?
            }
        };
        Ok(ColumnarValue::Array(result))
    }
}

static CYPHER_FIELD: LazyLock<Arc<ScalarUDF>> = LazyLock::new(|| {
    Arc::new(ScalarUDF::new_from_impl(CypherField {
        signature: Signature::any(2, Volatility::Immutable),
    }))
});

/// `base.name` on a map or temporal value.
pub(crate) fn field(base: Expr, name: &str) -> Expr {
    CYPHER_FIELD.call(vec![base, lit(name.to_string())])
}
