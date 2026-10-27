//! DataFusion bindings for [`crate::kernels`].
//!
//! Each UDF here is glue: a signature, a plan-time type check, and a call into a kernel. No
//! geometric work happens in this module.
//!
//! # Why `return_field_from_args`
//!
//! Every function that returns a geometry implements
//! [`return_field_from_args`][crate::datafusion::logical_expr::ScalarUDFImpl::return_field_from_args]
//! instead of `return_type`. A bare `DataType` cannot hold the GeoArrow extension metadata.
//! A function that used `return_type` would drop the coordinate reference system. The next
//! function in the chain could then not read the geometry. This is a correctness requirement,
//! not an optimization.
//!
//! # Which columns count as geometries
//!
//! A column with GeoArrow extension metadata is a geometry. GeoArrow also reads a plain `Utf8`
//! column as WKT, and a plain `Binary` column as WKB. So a raw text column from CSV works
//! without a cast. The cost is one surprise: `ST_AsText` on any string column returns that
//! string. Pass such a column through `ST_GeomFromText` to check the parse step.

#[macro_use]
pub mod macros;

pub mod accessor;
pub mod component;
pub mod constructor;
pub mod edit;
pub mod envelope;
pub mod io;
pub mod linear;
pub mod measure;
pub mod predicate;
pub mod process;
/// `ST_Transform`. Needs the `proj` feature.
#[cfg(feature = "proj")]
pub mod reproject;
pub mod signature;
pub mod transform;
pub mod util;

pub use accessor::{simple_accessors, st_m, st_x, st_y, st_z};
pub use component::components;
pub use constructor::constructors;
pub use edit::edits;
pub use envelope::envelopes;
pub use io::{io_functions, st_geomfromtext};
pub use linear::linear_functions;
pub use measure::measures;
pub use predicate::{predicates, st_intersects};
pub use process::processing;
#[cfg(feature = "proj")]
pub use reproject::reprojections;
pub use transform::transforms;
