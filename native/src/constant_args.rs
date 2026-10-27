//! Gives back the scalar arguments that the FFI expands to arrays.

use std::sync::Arc;

use beacon_plugin_sdk::datafusion::arrow::array::{Array, ArrayRef, UInt32Array, make_comparator};
use beacon_plugin_sdk::datafusion::arrow::compute::{SortOptions, take};
use beacon_plugin_sdk::datafusion::arrow::datatypes::{DataType, FieldRef};
use beacon_plugin_sdk::datafusion::common::config::ConfigOptions;
use beacon_plugin_sdk::datafusion::common::{ExprSchema, Result, ScalarValue};
use beacon_plugin_sdk::datafusion::logical_expr::interval_arithmetic::Interval;
use beacon_plugin_sdk::datafusion::logical_expr::preimage::PreimageResult;
use beacon_plugin_sdk::datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use beacon_plugin_sdk::datafusion::logical_expr::sort_properties::{
    ExprProperties, SortProperties,
};
use beacon_plugin_sdk::datafusion::logical_expr::{
    ColumnarValue, Documentation, Expr, ExpressionPlacement, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDF, ScalarUDFImpl, Signature, StructFieldMapping, Volatility,
};

/// A [`ScalarUDFImpl`] that turns a constant argument array back into a scalar.
///
/// `datafusion-ffi` sends each argument to the plugin as an array, also a literal.
/// The spatial functions need a scalar for some arguments, such as the SRID of `ST_SetSRID`.
/// They also have fast paths for a scalar geometry, such as a prepared polygon.
/// An array that holds one value in each row gives the same result as that value as a scalar.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ConstantArgs {
    inner: Arc<ScalarUDF>,
}

impl ConstantArgs {
    /// Wraps `udf`. A function that is not immutable stays as it is.
    pub fn wrap(udf: ScalarUDF) -> ScalarUDF {
        if udf.signature().volatility != Volatility::Immutable {
            return udf;
        }
        ScalarUDF::from(Self {
            inner: Arc::new(udf),
        })
    }

    fn imp(&self) -> &dyn ScalarUDFImpl {
        self.inner.inner().as_ref()
    }
}

/// Returns the value of `array` as a scalar when each row holds the same value.
fn as_constant(array: &ArrayRef) -> Option<ScalarValue> {
    if array.is_empty() {
        return None;
    }
    let same_as_first: Box<dyn Fn(usize) -> bool> =
        match make_comparator(array.as_ref(), array.as_ref(), SortOptions::default()) {
            Ok(compare) => Box::new(move |row| compare(0, row).is_eq()),
            // The comparator has no union support. Logical equality covers each type.
            Err(_) => {
                let first = array.slice(0, 1).to_data();
                Box::new(move |row| array.slice(row, 1).to_data() == first)
            }
        };
    if !(1..array.len()).all(same_as_first) {
        return None;
    }
    // A slice keeps the full child arrays. GeoArrow needs offsets that match the children.
    let first = take(array.as_ref(), &UInt32Array::from(vec![0]), None).ok()?;
    ScalarValue::try_from_array(first.as_ref(), 0).ok()
}

impl ScalarUDFImpl for ConstantArgs {
    fn name(&self) -> &str {
        self.imp().name()
    }

    fn aliases(&self) -> &[String] {
        self.imp().aliases()
    }

    #[allow(deprecated)]
    fn display_name(&self, args: &[Expr]) -> Result<String> {
        self.imp().display_name(args)
    }

    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        self.imp().schema_name(args)
    }

    fn signature(&self) -> &Signature {
        self.imp().signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.imp().return_type(arg_types)
    }

    fn with_updated_config(&self, config: &ConfigOptions) -> Option<ScalarUDF> {
        self.imp()
            .with_updated_config(config)
            .map(ConstantArgs::wrap)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        self.imp().return_field_from_args(args)
    }

    #[allow(deprecated)]
    fn is_nullable(&self, args: &[Expr], schema: &dyn ExprSchema) -> bool {
        self.imp().is_nullable(args, schema)
    }

    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        for arg in &mut args.args {
            if let ColumnarValue::Array(array) = arg
                && let Some(value) = as_constant(array)
            {
                *arg = ColumnarValue::Scalar(value);
            }
        }
        self.imp().invoke_with_args(args)
    }

    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        self.imp().simplify(args, info)
    }

    fn preimage(
        &self,
        args: &[Expr],
        lit_expr: &Expr,
        info: &SimplifyContext,
    ) -> Result<PreimageResult> {
        self.imp().preimage(args, lit_expr, info)
    }

    fn short_circuits(&self) -> bool {
        self.imp().short_circuits()
    }

    fn conditional_arguments<'a>(
        &self,
        args: &'a [Expr],
    ) -> Option<(Vec<&'a Expr>, Vec<&'a Expr>)> {
        self.imp().conditional_arguments(args)
    }

    fn evaluate_bounds(&self, input: &[&Interval]) -> Result<Interval> {
        self.imp().evaluate_bounds(input)
    }

    fn propagate_constraints(
        &self,
        interval: &Interval,
        inputs: &[&Interval],
    ) -> Result<Option<Vec<Interval>>> {
        self.imp().propagate_constraints(interval, inputs)
    }

    fn output_ordering(&self, inputs: &[ExprProperties]) -> Result<SortProperties> {
        self.imp().output_ordering(inputs)
    }

    fn preserves_lex_ordering(&self, inputs: &[ExprProperties]) -> Result<bool> {
        self.imp().preserves_lex_ordering(inputs)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        self.imp().coerce_types(arg_types)
    }

    fn struct_field_mapping(
        &self,
        literal_args: &[Option<ScalarValue>],
    ) -> Option<StructFieldMapping> {
        self.imp().struct_field_mapping(literal_args)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.imp().documentation()
    }

    fn placement(&self, args: &[ExpressionPlacement]) -> ExpressionPlacement {
        self.imp().placement(args)
    }
}
