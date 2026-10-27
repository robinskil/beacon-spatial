//! The pushdown end to end: from a spatial filter to skipped Parquet row groups.
//!
//! Beacon applies the hooks in `FunctionHooksRule` of `beacon-core`, which is not public.
//! [`HooksRule`] does the same core steps: it asks `implied` for each call in a filter, then
//! adds a `Necessary` predicate or replaces the call with an `Exact` one. DataFusion then
//! pushes the column ranges into the Parquet scan, which prunes row groups with statistics.

use std::sync::{Arc, OnceLock};

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Float64Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::common::Result;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::execution::{SessionStateBuilder, TaskContext};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::utils::{conjunction, split_conjunction_owned};
use datafusion::logical_expr::{Expr, Filter, LogicalPlan};
use datafusion::optimizer::AnalyzerRule;
use datafusion::optimizer::analyzer::type_coercion::TypeCoercionRewriter;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use spatial_native::datafusion;
use spatial_native::sdk::scalar_functions::{Implied, hooks_of};

/// The core of the Beacon hook rule, for the filters in these tests.
#[derive(Debug, Default)]
struct HooksRule {
    ctx: Arc<OnceLock<Arc<TaskContext>>>,
}

impl HooksRule {
    fn rewrite(&self, filter: Filter) -> Result<Transformed<LogicalPlan>> {
        let ctx = self.ctx.get().expect("the session sets the context");
        let mut conjuncts = Vec::new();
        let mut changed = false;
        for conjunct in split_conjunction_owned(filter.predicate.clone()) {
            let implied = match &conjunct {
                Expr::ScalarFunction(ScalarFunction { func, args }) => {
                    hooks_of(func).and_then(|hooked| hooked.implied(args, ctx).ok().flatten())
                }
                _ => None,
            };
            match implied {
                Some(Implied::Exact(expr)) => conjuncts.push(expr),
                Some(Implied::Necessary(expr)) => {
                    conjuncts.push(conjunct);
                    conjuncts.extend(split_conjunction_owned(expr));
                }
                None => {
                    conjuncts.push(conjunct);
                    continue;
                }
            }
            changed = true;
        }
        if !changed {
            return Ok(Transformed::no(LogicalPlan::Filter(filter)));
        }
        let schema = filter.input.schema().clone();
        let predicate = conjunction(conjuncts)
            .expect("a filter has a predicate")
            .rewrite(&mut TypeCoercionRewriter::new(&schema))?
            .data;
        Ok(Transformed::yes(LogicalPlan::Filter(Filter::try_new(
            predicate,
            filter.input,
        )?)))
    }
}

impl AnalyzerRule for HooksRule {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up(|node| match node {
            LogicalPlan::Filter(filter) => self.rewrite(filter),
            other => Ok(Transformed::no(other)),
        })
        .map(|transformed| transformed.data)
    }

    fn name(&self) -> &str {
        "test_function_hooks"
    }
}

/// A session with the plugin functions and table `points`: one Parquet file with four row
/// groups, one per quadrant. Rows 7 and 8 are in the north-east group.
async fn session(dir: &std::path::Path, with_rule: bool) -> SessionContext {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("lon", DataType::Float64, false),
        Field::new("lat", DataType::Float64, false),
    ]));
    let path = dir.join("points.parquet");
    if !path.exists() {
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(2))
            .build();
        let file = std::fs::File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(properties)).unwrap();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from_iter_values(1..=8)),
                Arc::new(Float64Array::from(vec![
                    -9.0, -6.0, 6.0, 9.0, -9.0, -6.0, 6.0, 9.0,
                ])),
                Arc::new(Float64Array::from(vec![
                    -9.0, -6.0, -9.0, -6.0, 6.0, 9.0, 6.0, 9.0,
                ])),
            ],
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    let rule = HooksRule::default();
    let task_ctx = Arc::clone(&rule.ctx);
    let mut builder = SessionStateBuilder::new().with_default_features();
    if with_rule {
        builder = builder.with_analyzer_rule(Arc::new(rule));
    }
    let ctx = SessionContext::new_with_state(builder.build());
    crate::common::register_all(&ctx);
    task_ctx.set(ctx.task_ctx()).unwrap();
    ctx.register_parquet(
        "points",
        path.to_str().unwrap(),
        ParquetReadOptions::default(),
    )
    .await
    .unwrap();
    ctx
}

async fn explain(ctx: &SessionContext, sql: &str) -> String {
    let batches = ctx
        .sql(&format!("EXPLAIN {sql}"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    datafusion::arrow::util::pretty::pretty_format_batches(&batches)
        .unwrap()
        .to_string()
}

/// The `row_groups_pruned_statistics` metric of `sql`, such as `4 total → 1 matched`.
async fn pruned(ctx: &SessionContext, sql: &str) -> String {
    let text = explain(ctx, &format!("ANALYZE {sql}")).await;
    let start = text
        .find("row_groups_pruned_statistics=")
        .unwrap_or_else(|| panic!("no pruning metric in {text}"));
    text[start + "row_groups_pruned_statistics=".len()..]
        .split([',', ']'])
        .next()
        .unwrap()
        .trim()
        .to_string()
}

async fn ids(ctx: &SessionContext, sql: &str) -> Vec<i64> {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    let mut ids: Vec<i64> = batches
        .iter()
        .flat_map(|b| b.column(0).as_primitive::<Int64Type>().values().to_vec())
        .collect();
    ids.sort();
    ids
}

/// Checks the plan, the pruning metric and the rows of `filter` on table `points`.
///
/// The rows must match the same query without the rule.
async fn check(filter: &str, call_stays: bool, matched: &str) -> Vec<i64> {
    let dir = tempfile::tempdir().unwrap();
    let sql = format!("SELECT id FROM points WHERE {filter}");
    let ctx = session(dir.path(), true).await;

    let plan = explain(&ctx, &sql).await;
    let function = filter[..filter.find('(').unwrap()].to_lowercase();
    assert_eq!(plan.contains(&format!("{function}(")), call_stays, "{plan}");
    assert_eq!(pruned(&ctx, &sql).await, matched, "{sql}");

    let rows = ids(&ctx, &sql).await;
    let plain = session(dir.path(), false).await;
    assert_eq!(rows, ids(&plain, &sql).await, "{sql}");
    rows
}

#[tokio::test]
async fn a_polygon_keeps_the_call_and_prunes_to_its_box() {
    let filter = "ST_Intersects(ST_Point(lon, lat), \
                  ST_GeomFromText('POLYGON((4 4,11 4,4 11,4 4))'))";
    let rows = check(filter, true, "4 total → 1 matched").await;
    assert_eq!(rows, [7]);
}

#[tokio::test]
async fn the_ranges_reach_the_parquet_scan() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = session(dir.path(), true).await;
    let sql = "SELECT id FROM points WHERE ST_Within(ST_Point(lon, lat), \
               ST_GeomFromText('POLYGON((4 4,11 4,4 11,4 4))'))";
    let plan = explain(&ctx, sql).await;
    let scan = plan
        .lines()
        .find(|line| line.contains("DataSourceExec") && line.contains("predicate="))
        .unwrap_or_else(|| panic!("no scan predicate in {plan}"));
    for range in ["lon@1 >= 4", "lon@1 <= 11", "lat@2 >= 4", "lat@2 <= 11"] {
        assert!(scan.contains(range), "{range} is not in {scan}");
    }
}

#[tokio::test]
async fn a_rectangle_replaces_the_call() {
    let filter = "ST_Intersects(ST_Point(lon, lat), ST_MakeEnvelope(4, 4, 11, 11))";
    // The statistics prove each row of the group true, so the scan checks no row.
    let rows = check(filter, false, "4 total → 1 matched -> 1 fully matched").await;
    assert_eq!(rows, [7, 8]);
}

#[tokio::test]
async fn bbox_intersects_replaces_the_call() {
    let filter = "ST_BBoxIntersects(ST_Point(lon, lat), \
                  ST_GeomFromText('POLYGON((4 4,11 4,4 11,4 4))'))";
    let rows = check(filter, false, "4 total → 1 matched -> 1 fully matched").await;
    assert_eq!(rows, [7, 8]);
}

#[tokio::test]
async fn contains_prunes_with_the_point_second() {
    let filter = "ST_Contains(ST_GeomFromText('POLYGON((-11 4,11 4,11 11,-11 11,-11 4))'), \
                  ST_Point(lon, lat))";
    let rows = check(filter, true, "4 total → 2 matched").await;
    assert_eq!(rows, [5, 6, 7, 8]);
}

#[tokio::test]
async fn dwithin_prunes_to_the_grown_box() {
    let filter = "ST_DWithin(ST_Point(lon, lat), ST_GeomFromText('POINT(4 4)'), 3)";
    let rows = check(filter, true, "4 total → 1 matched").await;
    assert_eq!(rows, [7]);
}

#[tokio::test]
async fn disjoint_prunes_nothing() {
    let filter = "ST_Disjoint(ST_Point(lon, lat), ST_MakeEnvelope(4, 4, 11, 11))";
    let rows = check(filter, true, "4 total → 4 matched").await;
    assert_eq!(rows, [1, 2, 3, 4, 5, 6]);
}
