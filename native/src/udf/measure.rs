//! Measurement functions as DataFusion scalar UDFs.

use std::sync::Arc;

use crate::datafusion::common::Result;
use crate::datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
};
use crate::kernels::measure::{BinaryMeasure, UnaryMeasure, binary_measure, unary_measure};
use arrow_schema::{DataType, Field, FieldRef};

use crate::udf::signature;
use crate::udf::util::{all_scalar, check_same_crs, geo_array, geo_type, to_df, wrap_result};

/// `ST_Area`, `ST_Length` or `ST_Perimeter`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct UnaryMeasureUdf {
    measure: UnaryMeasure,
    signature: Signature,
}

impl UnaryMeasureUdf {
    /// Build the UDF for one measurement.
    pub fn new(measure: UnaryMeasure) -> Self {
        Self {
            measure,
            signature: signature::geometries(1),
        }
    }
}

impl ScalarUDFImpl for UnaryMeasureUdf {
    fn name(&self) -> &str {
        self.measure.sql_name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        geo_type(self.measure.function_name(), 0, &args.arg_fields[0])?;
        Ok(Arc::new(Field::new(
            self.measure.sql_name(),
            DataType::Float64,
            true,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar_input = all_scalar(&args.args);
        let array = geo_array(&args.args[0], &args.arg_fields[0])?;
        let result = unary_measure(array.as_ref(), self.measure).map_err(to_df)?;
        wrap_result(Arc::new(result), scalar_input)
    }
}

/// `ST_Distance` and the other two-argument measurements.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct BinaryMeasureUdf {
    measure: BinaryMeasure,
    signature: Signature,
}

impl BinaryMeasureUdf {
    /// Build the UDF for one measurement.
    pub fn new(measure: BinaryMeasure) -> Self {
        Self {
            measure,
            signature: signature::geometries(2),
        }
    }
}

impl ScalarUDFImpl for BinaryMeasureUdf {
    fn name(&self) -> &str {
        self.measure.sql_name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let name = self.measure.function_name();
        let left = geo_type(name, 0, &args.arg_fields[0])?;
        let right = geo_type(name, 1, &args.arg_fields[1])?;
        check_same_crs(name, &left, &right)?;
        Ok(Arc::new(Field::new(
            self.measure.sql_name(),
            DataType::Float64,
            true,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar_input = all_scalar(&args.args);
        let left = geo_array(&args.args[0], &args.arg_fields[0])?;
        let right = geo_array(&args.args[1], &args.arg_fields[1])?;
        let result = binary_measure(left.as_ref(), right.as_ref(), self.measure).map_err(to_df)?;
        wrap_result(Arc::new(result), scalar_input)
    }
}

/// Every measurement function.
pub fn measures() -> Vec<ScalarUDF> {
    let mut functions: Vec<ScalarUDF> = UnaryMeasure::ALL
        .into_iter()
        .map(|measure| ScalarUDF::new_from_impl(UnaryMeasureUdf::new(measure)))
        .collect();
    functions.extend(
        BinaryMeasure::ALL
            .into_iter()
            .map(|measure| ScalarUDF::new_from_impl(BinaryMeasureUdf::new(measure))),
    );
    functions
}
