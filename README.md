# beacon-spatial

A [Beacon](https://github.com/maris-development/beacon) plugin with PostGIS-compatible spatial functions.
The functions work on GeoArrow, WKB (`Binary`) and WKT (`Utf8`) columns.
The code started as a port of [datafusion-spatial](https://github.com/robinskil/datafusion-spatial).
The plugin does not depend on that crate.

Read the Beacon guide `docs/docs/2.1.0-rc1/guides/write-a-plugin.md` for the plugin rules.

## Contents

| Path | Contents |
| --- | --- |
| `beacon-plugin.toml` | The plugin manifest: name, version, supported Beacon versions and files. |
| `native/src/kernels/` | The spatial kernels: array in, array out. They have no query engine code. |
| `native/src/udf/` | The DataFusion functions around the kernels: signatures, type checks and calls. |
| `native/src/pushdown.rs` | The planner hooks. They turn spatial filters into column ranges. |
| `native/tests/spatial/` | SQL tests. They load the functions through the FFI, as Beacon does. |
| `native/Cargo.lock` | The lock file. The package script reads the DataFusion version from it. |
| `scripts/package-plugin.py` | Packs the plugin into an archive and writes `SHA256SUMS`. |
| `.github/workflows/release.yml` | Builds and packs the plugin for a tag. Attaches the archives to the GitHub release. |

## Functions

The plugin `spatial` adds 117 scalar functions, for example
`ST_GeomFromText`, `ST_Intersects`, `ST_Buffer`, `ST_Area` and `ST_AsText`.
The [function reference](#function-reference) lists each function.

The function names follow PostGIS, so they do not start with the plugin name.
Beacon refuses a plugin function that has the name of a built-in function.
Thus install this plugin only on a Beacon build without built-in spatial functions.

A column with GeoArrow extension metadata is a geometry.
A plain `Binary` column is WKB, and a plain `Utf8` column is WKT.
So a raw text column from CSV works without a cast.

### Limits

| Item | Limit |
| --- | --- |
| `ST_Extent`, `ST_Collect`, `ST_MemUnion` | Not in the plugin. They are aggregate functions. The SDK exports scalar functions only. |
| `ST_ClusterDBSCAN`, `ST_ClusterKMeans` | Not in the plugin. They are window functions. |
| Spatial join | Not in the plugin. It is a physical optimizer rule, not a function. |
| `ST_Transform` | Only with the Cargo feature `proj` or `proj-bundled`. The release build has no PROJ. |
| `SHOW FUNCTIONS` | Does not list the plugin functions. The FFI signature has no example argument types. |

### Constant arguments

Beacon sends each literal argument to the plugin as a scalar.
Some functions need a constant, for example the SRID of `ST_SetSRID`.
Some functions have a fast path for a constant geometry, for example a prepared polygon in `ST_Intersects`.
A column argument arrives as an array, also when each row holds the same value.

The plugin uses plugin SDK ABI 4. Beacon refuses a plugin with a different ABI.

## Pushdown

DataFusion cannot see into a plugin function. So a spatial filter prunes no files by itself.
The planner hooks in `native/src/pushdown.rs` give Beacon plain column ranges.

A hook needs a point from two columns and a constant geometry:

```sql
SELECT * FROM argo
WHERE ST_Intersects(ST_Point(lon, lat), ST_GeomFromText('POLYGON((2 50,8 50,8 56,2 50))'))
```

Beacon rewrites the filter before the optimizer runs:

```sql
WHERE ST_Intersects(ST_Point(lon, lat), ST_GeomFromText('POLYGON((2 50,8 50,8 56,2 50))'))
  AND lon >= 2 AND lon <= 8 AND lat >= 50 AND lat <= 56
```

DataFusion then pushes the four ranges into the scan.
The scan compares them with the minimum and maximum of `lon` and `lat` in each file and row group.
It skips each file or row group outside the box.

### Pushdown levels

| Level | Rewrite | Effect on the scan |
| --- | --- | --- |
| **Exact** | Beacon replaces the call with the ranges. | The scan skips the groups outside the box. A group inside the box needs no row check. No geometry is built. |
| **Box** | Beacon keeps the call and adds the ranges. | The scan skips the groups outside the box. The call still checks each row of the other groups. |
| **Box grown by `d`** | As **Box**, with `d` added on each side. | As **Box**. |
| **No** | No change. | The scan reads each group. The call checks each row. |

The **Pushdown** column of the [function reference](#function-reference) gives the level of each predicate.
An empty cell means that the function is not a filter predicate.

### Measured pruning

`native/tests/spatial/scan_pruning.rs` runs each level on one Parquet file.
The file has four row groups, one per quadrant. The metric comes from `EXPLAIN ANALYZE`.

| Filter | Level | Row groups |
| --- | --- | --- |
| `ST_Intersects(ST_Point(lon, lat), ST_MakeEnvelope(4, 4, 11, 11))` | Exact | 4 total, 1 matched, 1 fully matched |
| `ST_BBoxIntersects(ST_Point(lon, lat), <triangle>)` | Exact | 4 total, 1 matched, 1 fully matched |
| `ST_Intersects(ST_Point(lon, lat), <triangle>)` | Box | 4 total, 1 matched |
| `ST_Contains(<rectangle over two groups>, ST_Point(lon, lat))` | Box | 4 total, 2 matched |
| `ST_DWithin(ST_Point(lon, lat), 'POINT(4 4)', 3)` | Box grown by `d` | 4 total, 1 matched |
| `ST_Disjoint(ST_Point(lon, lat), ST_MakeEnvelope(4, 4, 11, 11))` | No | 4 total, 4 matched |

"Fully matched" means that the statistics prove the filter true for each row of the group.
The amount of pruning depends on the data layout.
Data sorted or partitioned by position gives tight ranges per group, and most groups are skipped.
Data in random order gives wide ranges per group, and few groups are skipped.

### Conditions

- The point is `ST_Point`, `ST_MakePoint` or `ST_PointZ`. `ST_SetSRID` around the point is accepted.
- The coordinates can be any expression. They can also call a plugin function.
  Only a plain column gets file pruning.
- The geometry is any expression without a column, for example a WKT literal or `ST_MakeEnvelope(...)`.
  It can call any function that Beacon knows, also a function of another plugin.
- The call must be a condition of the filter, joined with `AND`.
  `NOT ST_Intersects(...)`, a call inside `OR` and `ST_Intersects(...) = true` get no ranges.
- A filter on a geometry column gets no ranges. Its pruning needs the bounding box statistics of the file format.
- A numeric result in a comparison gets no ranges, for example `ST_Distance(...) <= 1` or `ST_X(...) > 5`.
  Use `ST_DWithin` for a distance filter.

### Tests

- `native/tests/spatial/pushdown.rs` checks each rule against the call on rows with `-0.0`, `NaN`, nulls and edge points.
  A box rule must be true on each row where the call is true. An exact rule must give the same value as the call.
- `native/tests/spatial/scan_pruning.rs` checks the plan, the pruning metric and the rows of each level.
  The rows must be the same as the rows of the query without pushdown.
  The test uses a copy of the core steps of the Beacon hook rule, because that rule is not public.

## Function reference

Call each function by its PostGIS name.

| | Meaning |
|---|---|
| ✅ | The name, the arguments and the behaviour match PostGIS. |
| ⚠️ | The function works. Read the note. The arguments or the behaviour differ. |

### Accessors

| Function | PostGIS | | Pushdown | Notes |
|---|---|:--:|---|---|
| `ST_X(geom)` | `ST_X(geom)` | ✅ |  | Zero copy on a point column with separated coordinates. |
| `ST_Y(geom)` | `ST_Y(geom)` | ✅ |  |  |
| `ST_Z(geom)` | `ST_Z(geom)` | ✅ |  | Returns null for a 2D geometry. |
| `ST_M(geom)` | `ST_M(geom)` | ✅ |  | Returns null without a measure. |
| `ST_SRID(geom)` | `ST_SRID(geom)` | ⚠️ |  | Reads the column metadata. Every row returns the same value. |
| `ST_GeometryType(geom)` | `ST_GeometryType(geom)` | ✅ |  | The schema answers this for a single-type column. |
| `ST_Dimension(geom)` | `ST_Dimension(geom)` | ✅ |  |  |
| `ST_CoordDim(geom)` | `ST_CoordDim(geom)` | ✅ |  |  |
| `ST_NPoints(geom)` | `ST_NPoints(geom)` | ✅ |  |  |
| `ST_NumPoints(geom)` | `ST_NumPoints(geom)` | ✅ |  | Accepts a line string only. Other types return null. PostGIS does the same. |
| `ST_NumGeometries(geom)` | `ST_NumGeometries(geom)` | ✅ |  |  |
| `ST_NumInteriorRings(geom)` | `ST_NumInteriorRings(geom)` | ✅ |  |  |
| `ST_IsEmpty(geom)` | `ST_IsEmpty(geom)` | ✅ |  |  |
| `ST_IsClosed(geom)` | `ST_IsClosed(geom)` | ✅ |  |  |
| `ST_IsRing(geom)` | `ST_IsRing(geom)` | ✅ |  |  |
| `ST_IsSimple(geom)` | `ST_IsSimple(geom)` | ⚠️ |  | Follows the JTS rule. An areal geometry is always simple. Use `ST_IsValid` for those. |

### Components

| Function | PostGIS | | Pushdown | Notes |
|---|---|:--:|---|---|
| `ST_StartPoint(geom)` | `ST_StartPoint(geom)` | ✅ |  |  |
| `ST_EndPoint(geom)` | `ST_EndPoint(geom)` | ✅ |  |  |
| `ST_PointN(geom, n)` | `ST_PointN(geom, n)` | ✅ |  | The index starts at 1. An index outside the line returns null. |
| `ST_ExteriorRing(geom)` | `ST_ExteriorRing(geom)` | ✅ |  |  |
| `ST_InteriorRingN(geom, n)` | `ST_InteriorRingN(geom, n)` | ✅ |  |  |
| `ST_GeometryN(geom, n)` | `ST_GeometryN(geom, n)` | ✅ |  | Index 1 returns the input itself when the input is not a collection. |

### Constructors

| Function | PostGIS | | Pushdown | Notes |
|---|---|:--:|---|---|
| `ST_Point(x, y[, z])` | `ST_Point(x, y)` | ⚠️ |  | The function adopts the input buffers and copies nothing. A third argument sets z, not the SRID. |
| `ST_MakePoint(x, y[, z])` | `ST_MakePoint(x, y[, z])` | ✅ |  |  |
| `ST_PointZ(x, y[, z])` | `ST_PointZ(x, y, z)` | ⚠️ |  | This name calls `ST_MakePoint`. Two arguments return a 2D point. PostGIS needs three arguments. |
| `ST_MakeLine(a, b)` | `ST_MakeLine(a, b)` | ⚠️ |  | Joins two point columns into two-point lines. The plugin has no aggregate form and no array form. |
| `ST_MakePolygon(ring)` | `ST_MakePolygon(ring)` | ⚠️ |  | Builds the shell only. The plugin does not accept the second argument for holes. |
| `ST_MakeEnvelope(x1, y1, x2, y2)` | `ST_MakeEnvelope(x1, y1, x2, y2)` | ⚠️ |  | The function adopts all four input buffers. The plugin does not accept the fifth `srid` argument. |
| `ST_MakeBox2D(x1, y1, x2, y2)` | `ST_MakeBox2D(pt1, pt2)` | ⚠️ |  | **The arguments differ.** PostGIS takes two points. The plugin takes four ordinates. |

### Input and output

| Function | PostGIS | | Pushdown | Notes |
|---|---|:--:|---|---|
| `ST_AsText(geom)` | `ST_AsText(geom)` | ⚠️ |  | The plugin does not accept the `maxdecimaldigits` argument. |
| `ST_AsBinary(geom)` | `ST_AsBinary(geom)` | ✅ |  |  |
| `ST_AsEWKB(geom)` | `ST_AsEWKB(geom)` | ✅ |  | The SRID comes from the column metadata. |
| `ST_AsGeoJSON(geom)` | `ST_AsGeoJSON(geom)` | ⚠️ |  | Accepts a geometry only. The plugin does not accept the row form or the options argument. |
| `ST_GeomFromText(wkt)` | `ST_GeomFromText(wkt)` | ⚠️ |  | The plugin does not accept the `srid` argument. Call `ST_SetSRID` after it. |
| `ST_GeomFromWKB(bytea)` | `ST_GeomFromWKB(bytea)` | ✅ |  |  |
| `ST_GeomFromEWKB(bytea)` | `ST_GeomFromEWKB(bytea)` | ⚠️ |  | Reads the extended format. **The function loses the SRID inside the value.** Call `ST_SetSRID` after it. |
| `ST_GeomFromGeoJSON(json)` | `ST_GeomFromGeoJSON(json)` | ✅ |  |  |
| `ST_GeoHash(geom[, prec])` | `ST_GeoHash(geom[, prec])` | ⚠️ |  | Accepts a point only. Other types return null. The default precision is 20. |
| `ST_PointFromGeoHash(hash)` | `ST_PointFromGeoHash(hash)` | ⚠️ |  | Returns the centre of the cell. The plugin does not accept the precision argument. |

### Predicates

| Function | PostGIS | | Pushdown | Notes |
|---|---|:--:|---|---|
| `ST_Intersects(a, b)` | `ST_Intersects(a, b)` | ✅ | **Exact** for a rectangle. Else box. | The box test runs first. A point against a constant polygon takes the edge index. |
| `ST_Disjoint(a, b)` | `ST_Disjoint(a, b)` | ✅ | No | Two separate boxes prove this predicate true. Otherwise as `ST_Intersects`. |
| `ST_Contains(a, b)` | `ST_Contains(a, b)` | ✅ | Box | The box of `b` must sit inside the box of `a`. A point against a constant polygon takes the edge index. |
| `ST_ContainsProperly(a, b)` | `ST_ContainsProperly(a, b)` | ✅ | Box | Same as `ST_Contains` for a point argument. |
| `ST_Within(a, b)` | `ST_Within(a, b)` | ✅ | Box | The converse of `ST_Contains`, and it takes the same edge index. |
| `ST_Covers(a, b)` | `ST_Covers(a, b)` | ✅ | **Exact** for `(rectangle, point)`. Else box. | The boundary counts. A point against a constant polygon takes the edge index. |
| `ST_CoveredBy(a, b)` | `ST_CoveredBy(a, b)` | ✅ | **Exact** for `(point, rectangle)`. Else box. | The converse of `ST_Covers`, and it takes the same edge index. |
| `ST_Touches(a, b)` | `ST_Touches(a, b)` | ✅ | Box | Uses the DE-9IM matrix. A constant argument gets the cached R-tree. |
| `ST_Crosses(a, b)` | `ST_Crosses(a, b)` | ✅ | Box |  |
| `ST_Overlaps(a, b)` | `ST_Overlaps(a, b)` | ✅ | Box |  |
| `ST_Equals(a, b)` | `ST_Equals(a, b)` | ✅ | Box | Tests topological equality. The two boxes must match. |
| `ST_Relate(a, b)` | `ST_Relate(a, b)` | ✅ | No | Returns the nine character matrix. |
| `ST_Relate(a, b, pattern)` | `ST_Relate(a, b, pattern)` | ⚠️ | No | The pattern must be a constant. The plugin does not accept the `boundaryNodeRule` form. |
| `ST_DWithin(a, b, d)` | `ST_DWithin(a, b, d)` | ⚠️ | Box grown by `d` | `d` must be a constant. It grows the box that drives the prefilter. |
| `ST_DFullyWithin(a, b, d)` | `ST_DFullyWithin(a, b, d)` | ⚠️ | Box grown by `d` | `d` must be a constant. |
| `ST_BBoxIntersects(a, b)` | `geom_a && geom_b` | ⚠️ | **Exact** | **This is a function, not an operator.** DataFusion has no user operators. |

### Measurement

| Function | PostGIS | | Pushdown | Notes |
|---|---|:--:|---|---|
| `ST_Area(geom)` | `ST_Area(geom)` | ⚠️ |  | Planar. Longitude and latitude data returns square degrees. The PostGIS `geometry` type does the same. |
| `ST_Length(geom)` | `ST_Length(geom)` | ⚠️ |  | Planar. Measures lineal parts only. A polygon returns zero, as in PostGIS. |
| `ST_Perimeter(geom)` | `ST_Perimeter(geom)` | ⚠️ |  | Planar. Measures areal parts only. |
| `ST_Distance(a, b)` | `ST_Distance(a, b)` | ⚠️ |  | Planar. |
| `ST_MaxDistance(a, b)` | `ST_MaxDistance(a, b)` | ⚠️ |  | Exact. The cost is the product of the two vertex counts. |
| `ST_HausdorffDistance(a, b)` | `ST_HausdorffDistance(a, b)` | ⚠️ |  | The plugin does not accept the `densifyFrac` argument. |
| `ST_FrechetDistance(a, b)` | `ST_FrechetDistance(a, b)` | ⚠️ |  | Accepts a line string only. Other types return null. |
| `ST_DistanceSphere(a, b)` | `ST_DistanceSphere(a, b)` | ⚠️ |  | **Accepts two points only.** Other types return null. `geo` has no spherical nearest-point search. |
| `ST_DistanceSpheroid(a, b)` | `ST_DistanceSpheroid(a, b)` | ⚠️ |  | Accepts two points only. Uses WGS 84. The plugin does not accept a spheroid argument. |

### Linear reference

| Function | PostGIS | | Pushdown | Notes |
|---|---|:--:|---|---|
| `ST_ClosestPoint(a, b)` | `ST_ClosestPoint(a, b)` | ✅ |  |  |
| `ST_ShortestLine(a, b)` | `ST_ShortestLine(a, b)` | ✅ |  |  |
| `ST_LineLocatePoint(line, pt)` | `ST_LineLocatePoint(line, pt)` | ✅ |  | Accepts a line string only. Other types return null. |
| `ST_LineInterpolatePoint(line, f)` | `ST_LineInterpolatePoint(line, f)` | ✅ |  | A fraction outside 0 to 1 returns null. |

### Overlay

| Function | PostGIS | | Pushdown | Notes |
|---|---|:--:|---|---|
| `ST_Union(a, b)` | `ST_Union(a, b)` | ⚠️ |  | Accepts areal arguments only. Other types return null. Returns a mixed geometry column. |
| `ST_Intersection(a, b)` | `ST_Intersection(a, b)` | ⚠️ |  | Accepts areal arguments only. Other types return null. |
| `ST_Difference(a, b)` | `ST_Difference(a, b)` | ⚠️ |  | Accepts areal arguments only. Other types return null. |
| `ST_SymDifference(a, b)` | `ST_SymDifference(a, b)` | ⚠️ |  | Accepts areal arguments only. Other types return null. |

### Process

| Function | PostGIS | | Pushdown | Notes |
|---|---|:--:|---|---|
| `ST_Buffer(geom, d)` | `ST_Buffer(geom, d)` | ⚠️ |  | Uses round joins and round caps. The plugin does not accept `quad_segs` or a style string. |
| `ST_ConvexHull(geom)` | `ST_ConvexHull(geom)` | ✅ |  |  |
| `ST_ConcaveHull(geom, pct)` | `ST_ConcaveHull(geom, pct)` | ⚠️ |  | The plugin maps `target_percent` onto the `geo` concavity, which runs the other way. No hole support. |
| `ST_OrientedEnvelope(geom)` | `ST_OrientedEnvelope(geom)` | ✅ |  |  |
| `ST_Boundary(geom)` | `ST_Boundary(geom)` | ⚠️ |  | The plugin implements the OGC rule, because `geo` has no boundary algorithm. A collection returns an empty boundary. |
| `ST_Centroid(geom)` | `ST_Centroid(geom)` | ✅ |  |  |
| `ST_PointOnSurface(geom)` | `ST_PointOnSurface(geom)` | ✅ |  |  |
| `ST_Simplify(geom, tol)` | `ST_Simplify(geom, tol)` | ⚠️ |  | Uses Ramer-Douglas-Peucker. The plugin does not accept the `preserveCollapsed` flag. |
| `ST_SimplifyVW(geom, tol)` | `ST_SimplifyVW(geom, tol)` | ✅ |  | Uses Visvalingam-Whyatt. |
| `ST_Segmentize(geom, max)` | `ST_Segmentize(geom, max)` | ⚠️ |  | Planar. A length of zero or less returns null. |
| `ST_RemoveRepeatedPoints(geom)` | `ST_RemoveRepeatedPoints(geom)` | ⚠️ |  | The plugin does not accept a tolerance argument. |
| `ST_Reverse(geom)` | `ST_Reverse(geom)` | ✅ |  |  |
| `ST_ForcePolygonCCW(geom)` | `ST_ForcePolygonCCW(geom)` | ✅ |  |  |
| `ST_ForcePolygonCW(geom)` | `ST_ForcePolygonCW(geom)` | ✅ |  |  |
| `ST_Force2D(geom)` | `ST_Force2D(geom)` | ⚠️ |  | Accepts a native GeoArrow column only. Zero copy: the function drops one buffer handle. |
| `ST_Force3D(geom)` | `ST_Force3D(geom)` | ⚠️ |  | Accepts a native GeoArrow column only. The function adds a z of zero. |
| `ST_FlipCoordinates(geom)` | `ST_FlipCoordinates(geom)` | ✅ |  | Zero copy on separated coordinates. The function swaps two buffer handles. |
| `ST_SetSRID(geom, srid)` | `ST_SetSRID(geom, srid)` | ⚠️ |  | `srid` must be a constant, because it changes the column type. The function reads no row. |

### Validity

| Function | PostGIS | | Pushdown | Notes |
|---|---|:--:|---|---|
| `ST_IsValid(geom)` | `ST_IsValid(geom)` | ✅ |  |  |
| `ST_IsValidReason(geom)` | `ST_IsValidReason(geom)` | ⚠️ |  | A valid input returns `Valid Geometry`, as in PostGIS. The failure text comes from `geo`. |
| `ST_MakeValid(geom)` | `ST_MakeValid(geom)` | ⚠️ |  | Repairs an areal geometry. Other types pass through. The plugin does not accept an options argument. |

### Affine

| Function | PostGIS | | Pushdown | Notes |
|---|---|:--:|---|---|
| `ST_Translate(geom, dx, dy)` | `ST_Translate(geom, dx, dy)` | ⚠️ |  | The offsets must be constants. No 3D form. The output keeps the input geometry type. |
| `ST_Scale(geom, xf, yf)` | `ST_Scale(geom, xf, yf)` | ⚠️ |  | Scales about the origin. The plugin does not accept the factor-geometry form or the origin form. |
| `ST_Rotate(geom, rad)` | `ST_Rotate(geom, rad)` | ⚠️ |  | Takes radians. Rotates about the origin, as in PostGIS. No origin-point form. |
| `ST_Affine(geom, a,b,d,e,xoff,yoff)` | `ST_Affine(geom, a,b,d,e,xoff,yoff)` | ⚠️ |  | Supports the 2D form only. The plugin does not accept the twelve-argument 3D form. |

### Bounding box

| Function | PostGIS | | Pushdown | Notes |
|---|---|:--:|---|---|
| `ST_Envelope(geom)` | `ST_Envelope(geom)` | ⚠️ |  | Returns a GeoArrow box column. It reads as `ST_Polygon`. PostGIS returns a polygon geometry. |
| `ST_Expand(geom, d)` | `ST_Expand(geom, d)` | ⚠️ |  | Returns a box column. The plugin does not accept the per-axis form. |
| `ST_XMin(geom)` | `ST_XMin(box)` | ✅ |  | Zero copy on a box column. A box column holds four `f64` buffers. |
| `ST_YMin(geom)` | `ST_YMin(box)` | ✅ |  |  |
| `ST_XMax(geom)` | `ST_XMax(box)` | ✅ |  |  |
| `ST_YMax(geom)` | `ST_YMax(box)` | ✅ |  |  |
| `ST_ZMin(geom)` | `ST_ZMin(box)` | ⚠️ |  | Always returns null. The box pass is two-dimensional. |
| `ST_ZMax(geom)` | `ST_ZMax(box)` | ⚠️ |  | Always returns null. |

### Tessellation

| Function | PostGIS | | Pushdown | Notes |
|---|---|:--:|---|---|
| `ST_DelaunayTriangles(geom)` | `ST_DelaunayTriangles(geom)` | ⚠️ |  | Always returns a collection. Supports the unconstrained triangulation only. No tolerance and no flags. |
| `ST_VoronoiPolygons(geom)` | `ST_VoronoiPolygons(geom)` | ⚠️ |  | Clips each cell to the input extent plus 50 percent. PostGIS uses the same default. |
| `ST_VoronoiLines(geom)` | `ST_VoronoiLines(geom)` | ✅ |  |  |
| `ST_ChaikinSmoothing(geom, n)` | `ST_ChaikinSmoothing(geom, n)` | ⚠️ |  | The limit is eight iterations, because each one doubles the vertex count. No `preserve_end_points` flag. |

### Bearings

| Function | PostGIS | | Pushdown | Notes |
|---|---|:--:|---|---|
| `ST_Azimuth(a, b)` | `ST_Azimuth(a, b)` | ⚠️ |  | Returns radians clockwise from north, on WGS 84. Accepts two points only. Two equal points return null. |
| `ST_Project(pt, dist, azim)` | `ST_Project(pt, dist, azim)` | ⚠️ |  | Takes metres and radians on WGS 84. Accepts a point only. No two-point form. |

### Edits

| Function | PostGIS | | Pushdown | Notes |
|---|---|:--:|---|---|
| `ST_Multi(geom)` | `ST_Multi(geom)` | ✅ |  |  |
| `ST_Points(geom)` | `ST_Points(geom)` | ✅ |  | Returns every coordinate as one multi point. |
| `ST_SnapToGrid(geom, size)` | `ST_SnapToGrid(geom, size)` | ⚠️ |  | One grid size serves both axes. No origin form. A size of zero or less returns null. |
| `ST_AddPoint(line, pt[, pos])` | `ST_AddPoint(line, pt[, pos])` | ✅ |  | The index starts at 0. The function appends when you omit the position. |
| `ST_RemovePoint(line, pos)` | `ST_RemovePoint(line, pos)` | ⚠️ |  | The function returns null when the line would keep fewer than two vertices. |
| `ST_SetPoint(line, pos, pt)` | `ST_SetPoint(line, pos, pt)` | ✅ |  | Note the PostGIS order. The position comes before the point. |
| `ST_Dump(geom)` | `ST_Dump(geom)` | ⚠️ |  | **Returns a list of WKB, not a set.** Call `unnest` to expand it. Read the note below. |

### Reprojection

This function needs the `proj` feature. The release build has no PROJ.

| Function | PostGIS | | Pushdown | Notes |
|---|---|:--:|---|---|
| `ST_Transform(geom, srid)` | `ST_Transform(geom, srid)` | ⚠️ |  | The target SRID must be a constant. The source comes from the column metadata. No PROJ string form. |

### Not in the plugin

| Area | PostGIS functions | Reason |
|---|---|---|
| Aggregates | `ST_Extent`, `ST_Collect`, `ST_MemUnion` | The SDK exports scalar functions only. |
| Window functions | `ST_ClusterKMeans`, `ST_ClusterDBSCAN` | The SDK exports scalar functions only. |
| Geography type | every `geography` overload | The plugin needs a second type and a spherical algorithm set. |
| Reprojection extras | `ST_Transform(geom, to_proj)` | The function must accept a raw PROJ string. Only the argument parse step is absent. |
| Edits | `ST_Snap`, `ST_Node`, `ST_Split`, `ST_LineMerge`, `ST_LineSubstring`, `ST_Subdivide` | `geo` has no equivalent. Each one needs new code. |
| 3D | `ST_3DDistance`, `ST_3DIntersects`, `ST_3DLength` and the rest | `geo` is two-dimensional. |
| Other output | `ST_AsGML`, `ST_AsKML`, `ST_AsSVG`, `ST_AsMVT` | The plugin needs format writers. `geozero` covers several of them. |
| Topology-safe simplify | `ST_SimplifyPreserveTopology` | `geo` has no equivalent. |
| Set output | `ST_DumpPoints`, `ST_DumpRings` | These have the same shape as `ST_Dump`. `ST_Points` covers the common use. |

### The `ST_Dump` output

In PostGIS, `ST_Dump` returns a set. One input row becomes many output rows.
A DataFusion scalar function cannot return a set. So this function returns a list.
Call `unnest` to expand the list:

```sql
SELECT ST_AsText(unnest(ST_Dump(geom))) AS part FROM shapes
```

The parts are WKB, not GeoArrow geometries. The plugin has no choice here.
GeoArrow marks a column as spatial through the field metadata.
The DataFusion `unnest` step drops the metadata on the child field of the list.
A list of geometries then arrives as a plain struct. No spatial function accepts it.

WKB avoids the problem. A plain `Binary` column is always WKB under the rule below.
So the parts stay usable without a cast.

## Choose the versions

`native/Cargo.toml` gets `beacon-plugin-sdk` from the Beacon Git repository.
The Beacon ref fixes the DataFusion version of the plugin.
Beacon refuses a plugin with a different DataFusion major version.

The plugin uses the branch `main`, because no Beacon release holds the SDK yet.
Set a Beacon release tag when one is available:

```toml
beacon-plugin-sdk = { git = "https://github.com/maris-development/beacon", tag = "v2.1.0" }
```

Then run `cargo update -p beacon-plugin-sdk` in `native/`.
Make `plugin.beacon` in `beacon-plugin.toml` match the Beacon versions of that DataFusion version.

The arrow crates in `native/Cargo.toml` must have the arrow major of the SDK DataFusion.
geoarrow must use that arrow major too.
`native/Cargo.lock` must hold one `datafusion` package. The package script stops when it finds two.

Use the `datafusion` crate that the SDK re-exports. Do not add `datafusion` as a dependency.
Keep `panic = "unwind"`. This is the default. The SDK refuses `panic = "abort"`.

## Run the unit tests

The SQL tests load the functions through the FFI, as Beacon does. They need no Beacon server.

```bash
cargo test --manifest-path native/Cargo.toml
```

## Pack the plugin

Do these steps on Linux. The Beacon server image is a Linux image, so the library must be a Linux `.so` file.
On Windows, use WSL.

1. Build the library:

   ```bash
   cargo build --release --manifest-path native/Cargo.toml
   ```

2. Pack the plugin. The script writes the archive and `SHA256SUMS` in `dist/`:

   ```bash
   python3 scripts/package-plugin.py . --out dist --library native/target/release/libspatial_native.so --target x86_64-unknown-linux-gnu
   ```

| Plugin type | Archive name |
| --- | --- |
| native | `<name>-<version>-df<major>-<target>.tar.gz` |
| SQL only | `<name>-<version>-any.tar.gz` |

To try the archive, mount `dist/` into a Beacon container as `/plugins`. Then run:

```sql
INSTALL spatial FROM 'file:///plugins/spatial-0.1.0-df54-x86_64-unknown-linux-gnu.tar.gz'
```

## Release the plugin

1. Set `plugin.version` in `beacon-plugin.toml`.
2. Commit the change.
3. Push a tag `v<version>`:

   ```bash
   git tag v0.1.0
   ```

   ```bash
   git push origin v0.1.0
   ```

The workflow `release.yml` stops if the tag does not match `plugin.version`.
It builds the library for `x86_64-unknown-linux-gnu` and `aarch64-unknown-linux-gnu`.
Then it attaches the archives and one `SHA256SUMS` file to the release `v0.1.0`.
The workflow runs no tests. Run the unit tests before you push the tag.

## Install the plugin

An admin installs the plugin with SQL. Replace `<owner>/<repo>` with your repository:

```sql
INSTALL spatial FROM 'github:<owner>/<repo>@v0.1.0'
```

Beacon reads `SHA256SUMS` from the release and checks the archive.
It picks the archive for its own DataFusion version and target.

To install the plugin at server start, set `BEACON_PLUGINS`:

```bash
BEACON_PLUGINS=spatial=github:<owner>/<repo>@v0.1.0
```

`beacon.system.plugins` shows the installed plugins. `UNINSTALL spatial` removes the plugin.

## Rules for native code

- A fault in native code stops the Beacon server. Test the plugin well.
- A panic in a function becomes a query error.
- Do not panic in `name`, `aliases`, `signature` or `Drop`.

## License

Apache-2.0. See [LICENSE](LICENSE).
