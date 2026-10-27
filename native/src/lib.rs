//! Beacon plugin with PostGIS-compatible spatial functions on GeoArrow.

mod constant_args;
pub mod kernels;
mod pushdown;
pub mod udf;

pub use beacon_plugin_sdk as sdk;
pub use beacon_plugin_sdk::datafusion;

use std::sync::Arc;

use beacon_plugin_sdk::scalar_functions::HookedUdf;
use datafusion::logical_expr::ScalarUDF;

use constant_args::ConstantArgs;

/// Returns every scalar spatial function, such as `ST_Intersects` and `ST_AsText`.
pub fn functions() -> Vec<ScalarUDF> {
    let mut functions = vec![udf::st_x(), udf::st_y(), udf::st_z(), udf::st_m()];
    functions.extend(udf::simple_accessors());
    functions.extend(udf::components());
    functions.extend(udf::transforms());
    functions.extend(udf::constructors());
    functions.extend(udf::io_functions());
    functions.extend(udf::predicates());
    functions.extend(udf::measures());
    functions.extend(udf::linear_functions());
    functions.extend(udf::processing());
    functions.extend(udf::envelopes());
    functions.extend(udf::edits());
    #[cfg(feature = "proj")]
    functions.extend(udf::reprojections());
    functions
        .into_iter()
        .map(|udf| {
            let udf = ConstantArgs::wrap(udf);
            match pushdown::hooks_for(udf.name()) {
                Some(hooks) => ScalarUDF::from(HookedUdf::new(Arc::new(udf), hooks)),
                None => udf,
            }
        })
        .collect()
}

beacon_plugin_sdk::export_plugin!(scalar = functions);

#[cfg(test)]
mod tests {
    use beacon_plugin_sdk::{BEACON_PLUGIN_SDK_ABI, BeaconPluginAbi, datafusion_ffi};

    use super::beacon_plugin_abi;

    #[test]
    fn the_abi_matches_the_sdk() {
        let expected = BeaconPluginAbi {
            sdk_abi: BEACON_PLUGIN_SDK_ABI,
            ffi_major: datafusion_ffi::version(),
        };
        assert_eq!(beacon_plugin_abi(), expected);
    }
}
