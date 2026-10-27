//! Planner hooks that turn spatial filters on point columns into plain column ranges.
//!
//! DataFusion cannot see into a plugin function, so a spatial filter prunes no files by itself.
//! A filter such as `ST_Intersects(ST_Point(lon, lat), <constant geometry>)` is true only when
//! `lon` and `lat` are in the bounding box of the constant geometry. The `implied` hook gives
//! that box as `lon >= xmin AND lon <= xmax AND lat >= ymin AND lat <= ymax`. Beacon uses the
//! ranges to prune files and row groups with their statistics.

use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchOptions};
use arrow_schema::{DataType, FieldRef, Schema};
use beacon_plugin_sdk::scalar_functions::{FunctionHooks, Implied};
use geo::{BoundingRect, Geometry, Rect};

use crate::datafusion::common::{DFSchema, Result, ScalarValue};
use crate::datafusion::execution::TaskContext;
use crate::datafusion::logical_expr::{ColumnarValue, Expr, lit};
use crate::datafusion::physical_expr::create_physical_expr;
use crate::datafusion::physical_expr::execution_props::ExecutionProps;
use crate::kernels::predicate::geometry_at;
use crate::udf::util::{geo_array, to_df};

/// When a call can be replaced with the box test.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Exact {
    /// Never. The box test is only necessary.
    Never,
    /// When the geometry is an axis-aligned rectangle, at the given argument positions.
    Rectangle(Order),
    /// Always. The function is a box test.
    Always,
}

/// The argument order that an [`Exact::Rectangle`] rule needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Order {
    /// The point and the rectangle in either order.
    Any,
    /// The point first, the rectangle second.
    PointFirst,
    /// The rectangle first, the point second.
    PointSecond,
}

/// The pushdown rule of one function.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Rule {
    /// A predicate that is true only when the two geometries intersect.
    Intersection(Exact),
    /// `ST_DWithin` or `ST_DFullyWithin`: the box grows by the distance.
    Distance,
}

/// The planner hooks of one spatial predicate.
#[derive(Debug)]
pub struct SpatialHooks {
    rule: Rule,
}

/// Returns the hooks of the function `name`, or `None` when it has no pushdown rule.
pub fn hooks_for(name: &str) -> Option<Arc<dyn FunctionHooks>> {
    let rule = match name {
        "st_bboxintersects" => Rule::Intersection(Exact::Always),
        "st_intersects" => Rule::Intersection(Exact::Rectangle(Order::Any)),
        "st_coveredby" => Rule::Intersection(Exact::Rectangle(Order::PointFirst)),
        "st_covers" => Rule::Intersection(Exact::Rectangle(Order::PointSecond)),
        "st_contains"
        | "st_containsproperly"
        | "st_within"
        | "st_touches"
        | "st_crosses"
        | "st_overlaps"
        | "st_equals" => Rule::Intersection(Exact::Never),
        "st_dwithin" | "st_dfullywithin" => Rule::Distance,
        _ => return None,
    };
    Some(Arc::new(SpatialHooks { rule }))
}

impl FunctionHooks for SpatialHooks {
    fn implied(&self, args: &[Expr], _ctx: &Arc<TaskContext>) -> Result<Option<Implied>> {
        if args.len() < 2 {
            return Ok(None);
        }
        // Each order: the point constructor on one side, the constant geometry on the other.
        for (point, geometry, point_first) in
            [(&args[0], &args[1], true), (&args[1], &args[0], false)]
        {
            let Some((x, y)) = point_coordinates(point) else {
                continue;
            };
            let Some(geometry) = constant_geometry(geometry)? else {
                continue;
            };
            let Some(bbox) = geometry.bounding_rect() else {
                return Ok(None);
            };
            return Ok(match self.rule {
                Rule::Intersection(exact) => {
                    let range = in_box(x, y, bbox, 0.0);
                    let is_exact = match exact {
                        Exact::Never => false,
                        Exact::Always => true,
                        Exact::Rectangle(order) => {
                            let order_fits = match order {
                                Order::Any => true,
                                Order::PointFirst => point_first,
                                Order::PointSecond => !point_first,
                            };
                            order_fits && is_rectangle(&geometry, bbox)
                        }
                    };
                    Some(if is_exact {
                        Implied::Exact(range)
                    } else {
                        Implied::Necessary(range)
                    })
                }
                Rule::Distance => {
                    let Some(distance) = args.get(2).map(constant_f64).transpose()?.flatten()
                    else {
                        return Ok(None);
                    };
                    if !distance.is_finite() || distance < 0.0 {
                        return Ok(None);
                    }
                    Some(Implied::Necessary(in_box(x, y, bbox, distance)))
                }
            });
        }
        Ok(None)
    }
}

/// The `x` and `y` arguments of a point constructor, such as `ST_Point(lon, lat)`.
///
/// `ST_SetSRID` around the constructor keeps the coordinates.
fn point_coordinates(expr: &Expr) -> Option<(&Expr, &Expr)> {
    let Expr::ScalarFunction(call) = expr else {
        return None;
    };
    match call.func.name() {
        "st_point" | "st_makepoint" | "st_pointz" if call.args.len() >= 2 => {
            Some((&call.args[0], &call.args[1]))
        }
        "st_setsrid" => point_coordinates(call.args.first()?),
        _ => None,
    }
}

/// Evaluates an expression without columns, such as `ST_GeomFromText('POINT(1 2)')`.
///
/// Returns `None` for an expression that reads a column. Each call in the decoded expression
/// holds its function, so a function of Beacon or of another plugin also works.
fn evaluate_constant(expr: &Expr) -> Result<Option<(FieldRef, ColumnarValue)>> {
    if !expr.column_refs().is_empty() {
        return Ok(None);
    }
    let physical = create_physical_expr(expr, &DFSchema::empty(), &ExecutionProps::new())?;
    let batch = RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(1)),
    )?;
    let field = physical.return_field(batch.schema_ref())?;
    let value = physical.evaluate(&batch)?;
    Ok(Some((field, value)))
}

/// The value of a constant geometry expression, or `None` for a column or a null.
fn constant_geometry(expr: &Expr) -> Result<Option<Geometry<f64>>> {
    let Some((field, value)) = evaluate_constant(expr)? else {
        return Ok(None);
    };
    let array = geo_array(&value, &field)?;
    geometry_at(array.as_ref(), 0).map_err(to_df)
}

/// The value of a constant number expression, or `None` for a column or a null.
fn constant_f64(expr: &Expr) -> Result<Option<f64>> {
    let Some((_, value)) = evaluate_constant(expr)? else {
        return Ok(None);
    };
    let array = value.to_array(1)?;
    let ScalarValue::Float64(value) =
        ScalarValue::try_from_array(array.as_ref(), 0)?.cast_to(&DataType::Float64)?
    else {
        return Ok(None);
    };
    Ok(value)
}

/// True when `geometry` is the axis-aligned rectangle `bbox` with an area.
///
/// A point is then in the geometry exactly when it is in the box.
fn is_rectangle(geometry: &Geometry<f64>, bbox: Rect<f64>) -> bool {
    if bbox.width() <= 0.0 || bbox.height() <= 0.0 {
        return false;
    }
    match geometry {
        Geometry::Rect(_) => true,
        Geometry::Polygon(polygon) => {
            let (min, max) = (bbox.min(), bbox.max());
            let on_corner = |c: &geo::Coord<f64>| {
                (c.x == min.x || c.x == max.x) && (c.y == min.y || c.y == max.y)
            };
            let ring = &polygon.exterior().0;
            // Four distinct corners, joined by axis-aligned edges, make the four sides of the box.
            polygon.interiors().is_empty()
                && ring.len() == 5
                && ring.iter().all(on_corner)
                && ring
                    .windows(2)
                    .all(|e| e[0].x == e[1].x || e[0].y == e[1].y)
                && (0..4).all(|i| (i + 1..4).all(|j| ring[i] != ring[j]))
        }
        _ => false,
    }
}

/// `x` and `y` in `bbox`, grown by `margin` on each side.
fn in_box(x: &Expr, y: &Expr, bbox: Rect<f64>, margin: f64) -> Expr {
    let (min, max) = (bbox.min(), bbox.max());
    let side = |value: &Expr, low: f64, high: f64| {
        value
            .clone()
            .gt_eq(lit(lower(low - margin)))
            .and(value.clone().lt_eq(lit(upper(high + margin))))
    };
    side(x, min.x, max.x).and(side(y, min.y, max.y))
}

/// Arrow orders `-0.0` below `0.0`. A lower bound of zero must also hold `-0.0`.
fn lower(value: f64) -> f64 {
    if value == 0.0 { -0.0 } else { value }
}

/// An upper bound of zero must also hold `0.0`.
fn upper(value: f64) -> f64 {
    if value == 0.0 { 0.0 } else { value }
}
