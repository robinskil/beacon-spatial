//! The planner hooks: the column ranges that a spatial filter implies.
//!
//! The hooks run through the FFI, as Beacon calls them. Each rule is checked on real rows:
//! a `Necessary` rule must hold on every row where the call is true, and an `Exact` rule must
//! give the same value as the call on every row.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, Float64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{Expr, col, lit};
use datafusion::prelude::SessionContext;
use spatial_native::datafusion;
use spatial_native::sdk::scalar_functions::{Implied, hooks_of};

const TRIANGLE: &str = "POLYGON((0 0,2 0,0 2,0 0))";

/// A session with the plugin functions and a `points` table with edge cases.
fn session() -> SessionContext {
    let ctx = SessionContext::new();
    crate::common::register_all(&ctx);
    let lon = [
        0.5,
        -0.0,
        0.0,
        1.0,
        1.000_000_1,
        f64::NAN,
        0.5,
        5.0,
        -1.0,
        0.5,
        2.0,
        0.0,
    ];
    let lat = [0.5, 0.5, 0.0, 1.0, 0.5, 0.5, 0.5, 5.0, -1.0, 0.5, 0.0, -0.0];
    let lon_nulls = [6];
    let lat_nulls = [9];
    let column = |values: &[f64], nulls: &[usize]| {
        Float64Array::from_iter(
            values
                .iter()
                .enumerate()
                .map(|(i, v)| (!nulls.contains(&i)).then_some(*v)),
        )
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new("lon", DataType::Float64, true),
        Field::new("lat", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(column(&lon, &lon_nulls)),
            Arc::new(column(&lat, &lat_nulls)),
        ],
    )
    .unwrap();
    ctx.register_batch("points", batch).unwrap();
    ctx
}

/// A call to the plugin function `name`.
fn call(ctx: &SessionContext, name: &str, args: Vec<Expr>) -> Expr {
    ctx.udf(name).unwrap().call(args)
}

fn point(ctx: &SessionContext) -> Expr {
    call(ctx, "st_point", vec![col("lon"), col("lat")])
}

fn wkt(ctx: &SessionContext, text: &str) -> Expr {
    call(ctx, "st_geomfromtext", vec![lit(text)])
}

fn rectangle(ctx: &SessionContext) -> Expr {
    call(
        ctx,
        "st_makeenvelope",
        vec![lit(0.0), lit(0.0), lit(1.0), lit(1.0)],
    )
}

/// The `implied` answer for `name(args)`, through the FFI.
fn implied(ctx: &SessionContext, name: &str, args: &[Expr]) -> Option<Implied> {
    let udf = ctx.udf(name).unwrap();
    let hooks = hooks_of(&udf).unwrap_or_else(|| panic!("{name} has no hooks"));
    hooks.implied(args, &ctx.task_ctx()).unwrap()
}

/// Evaluates `call` and `predicate` on each row of `points`.
async fn evaluate(
    ctx: &SessionContext,
    call: Expr,
    predicate: Expr,
) -> Vec<(Option<bool>, Option<bool>)> {
    let batches = ctx
        .table("points")
        .await
        .unwrap()
        .select(vec![call.alias("call"), predicate.alias("predicate")])
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let (calls, predicates) = (batch.column(0).as_boolean(), batch.column(1).as_boolean());
        for row in 0..batch.num_rows() {
            let value =
                |array: &arrow_array::BooleanArray| (!array.is_null(row)).then(|| array.value(row));
            rows.push((value(calls), value(predicates)));
        }
    }
    rows
}

/// Checks the rule of `name(args)` on every row. Returns true when the rule is exact.
async fn check(ctx: &SessionContext, name: &str, args: Vec<Expr>) -> bool {
    let rule = implied(ctx, name, &args).unwrap_or_else(|| panic!("{name} gave no rule"));
    let (exact, predicate) = match rule {
        Implied::Exact(expr) => (true, expr),
        Implied::Necessary(expr) => (false, expr),
    };
    let rows = evaluate(ctx, call(ctx, name, args), predicate.clone()).await;
    for (row, (call, predicate_value)) in rows.iter().enumerate() {
        if exact {
            assert_eq!(
                call, predicate_value,
                "{name}: row {row} differs from {predicate}"
            );
        } else if *call == Some(true) {
            assert_eq!(
                *predicate_value,
                Some(true),
                "{name}: row {row} fails {predicate}"
            );
        }
    }
    exact
}

#[tokio::test]
async fn intersects_a_polygon_implies_its_box() {
    let ctx = session();
    let args = vec![point(&ctx), wkt(&ctx, TRIANGLE)];
    let Some(Implied::Necessary(predicate)) = implied(&ctx, "st_intersects", &args) else {
        panic!("expected a necessary rule");
    };
    let expected = col("lon")
        .gt_eq(lit(-0.0))
        .and(col("lon").lt_eq(lit(2.0)))
        .and(col("lat").gt_eq(lit(-0.0)).and(col("lat").lt_eq(lit(2.0))));
    assert_eq!(predicate, expected);
    assert!(!check(&ctx, "st_intersects", args).await);
}

#[tokio::test]
async fn intersects_a_rectangle_is_exact_in_both_orders() {
    let ctx = session();
    assert!(check(&ctx, "st_intersects", vec![point(&ctx), rectangle(&ctx)]).await);
    assert!(check(&ctx, "st_intersects", vec![rectangle(&ctx), point(&ctx)]).await);
}

#[tokio::test]
async fn a_rectangle_written_as_wkt_is_exact() {
    let ctx = session();
    let square = wkt(&ctx, "POLYGON((0 0,1 0,1 1,0 1,0 0))");
    assert!(check(&ctx, "st_intersects", vec![point(&ctx), square]).await);
}

#[tokio::test]
async fn a_bowtie_is_not_a_rectangle() {
    let ctx = session();
    let bowtie = wkt(&ctx, "POLYGON((0 0,1 1,0 1,1 0,0 0))");
    assert!(!check(&ctx, "st_intersects", vec![point(&ctx), bowtie]).await);
}

#[tokio::test]
async fn covers_is_exact_only_with_the_rectangle_first() {
    let ctx = session();
    assert!(check(&ctx, "st_covers", vec![rectangle(&ctx), point(&ctx)]).await);
    assert!(!check(&ctx, "st_covers", vec![point(&ctx), rectangle(&ctx)]).await);
    assert!(check(&ctx, "st_coveredby", vec![point(&ctx), rectangle(&ctx)]).await);
}

#[tokio::test]
async fn every_intersection_predicate_has_a_necessary_rule() {
    let ctx = session();
    for name in [
        "st_contains",
        "st_containsproperly",
        "st_within",
        "st_touches",
        "st_crosses",
        "st_overlaps",
        "st_equals",
    ] {
        for args in [
            vec![point(&ctx), wkt(&ctx, TRIANGLE)],
            vec![wkt(&ctx, TRIANGLE), point(&ctx)],
        ] {
            assert!(!check(&ctx, name, args).await, "{name} must not be exact");
        }
    }
}

#[tokio::test]
async fn bbox_intersects_is_always_exact() {
    let ctx = session();
    assert!(
        check(
            &ctx,
            "st_bboxintersects",
            vec![point(&ctx), wkt(&ctx, TRIANGLE)]
        )
        .await
    );
}

#[tokio::test]
async fn dwithin_grows_the_box_by_the_distance() {
    let ctx = session();
    let args = vec![point(&ctx), wkt(&ctx, "POINT(0 0)"), lit(1.5)];
    let Some(Implied::Necessary(predicate)) = implied(&ctx, "st_dwithin", &args) else {
        panic!("expected a necessary rule");
    };
    assert!(
        predicate.to_string().contains("lon <= Float64(1.5)"),
        "{predicate}"
    );
    assert!(!check(&ctx, "st_dwithin", args).await);
    let args = vec![wkt(&ctx, TRIANGLE), point(&ctx), lit(1_i64)];
    assert!(!check(&ctx, "st_dfullywithin", args).await);
}

#[tokio::test]
async fn a_negative_distance_gives_no_rule() {
    let ctx = session();
    let args = vec![point(&ctx), wkt(&ctx, "POINT(0 0)"), lit(-1.0)];
    assert_eq!(implied(&ctx, "st_dwithin", &args), None);
}

#[tokio::test]
async fn set_srid_around_the_point_keeps_the_rule() {
    let ctx = session();
    let point = call(&ctx, "st_setsrid", vec![point(&ctx), lit(4326)]);
    let polygon = call(&ctx, "st_setsrid", vec![wkt(&ctx, TRIANGLE), lit(4326)]);
    assert!(implied(&ctx, "st_intersects", &[point, polygon]).is_some());
}

#[tokio::test]
async fn no_rule_without_a_point_constructor_or_a_constant() {
    let ctx = session();
    // A geometry column has no column ranges.
    let column = col("lon");
    assert_eq!(
        implied(&ctx, "st_intersects", &[column, wkt(&ctx, TRIANGLE)]),
        None
    );
    // Two points that both read columns.
    assert_eq!(
        implied(&ctx, "st_intersects", &[point(&ctx), point(&ctx)]),
        None
    );
}

#[tokio::test]
async fn a_coordinate_can_call_a_plugin_function() {
    let ctx = session();
    let x = call(&ctx, "st_x", vec![point(&ctx)]);
    let nested = call(&ctx, "st_point", vec![x, col("lat")]);
    assert!(check(&ctx, "st_intersects", vec![nested, rectangle(&ctx)]).await);
}

#[tokio::test]
async fn disjoint_has_no_hooks() {
    let ctx = session();
    assert!(hooks_of(&ctx.udf("st_disjoint").unwrap()).is_none());
}
