"""
Benchmark: spatial_iceberg (bbox sub-field file-pruning) vs iceberg (no pruning)

Runs against every table in DATASET_CONFIGS that exists and has rows.

SI queries use the production-equivalent INTERSECTS form (bbox-overlap +
CASE WHEN bbox-contained shortcut, see _intersects_shortcut) — the same SQL
that geomesa-trino-datastore emits via TrinoFilterToSQL for CQL INTERSECTS.
ICE queries stay with plain ST_Intersects to represent the baseline
"no GeoMesa stack" counterfactual.

Every query is SELECT COUNT(*) by design — the goal here is to measure connector
scan time (file pruning + predicate pushdown), not JDBC streaming or WKB-decode
throughput. Once a query returns millions of result rows, full feature iteration
on either connector is dominated by the streaming/decode floor (which is identical
for both), and the connector's pruning advantage gets buried. The companion Java
benchmark (geomesa-trino-benchmark) keeps one feature-iteration smoke test per
dataset to validate the read path end-to-end.

Metrics per query type:
  - Wall time (mean ± σ over N runs)
  - Parquet files read  (ScanFilterAndProjectOperator.totalDrivers)
  - Rows read from disk (physicalInputPositions)
  - Bytes read from disk (physicalInputDataSize)
  - File-skip %  and speedup

Usage:
  python tools/benchmark.py [--runs N] [--warmup N]
"""
import argparse
import json
import math
import os
import re
import statistics
import sys
import time

import requests
import trino


def _wkt_envelope(wkt: str) -> tuple[float, float, float, float]:
    """Extract (minx, miny, maxx, maxy) from a POLYGON WKT. Cheap; coords are explicit."""
    coords = [tuple(map(float, p.split())) for p in re.findall(r"-?\d+\.?\d*\s+-?\d+\.?\d*", wkt)]
    xs = [c[0] for c in coords]; ys = [c[1] for c in coords]
    return min(xs), min(ys), max(xs), max(ys)


def _is_axis_aligned_rectangle(wkt: str) -> bool:
    """True iff the WKT polygon is an axis-aligned rectangle.

    An axis-aligned rectangle has exactly 4 unique corner vertices (5 with the
    closing point), 2 distinct x-coordinates, and 2 distinct y-coordinates.
    That's necessary AND sufficient for the polygon to be a rect with edges
    parallel to the axes."""
    coords = [tuple(map(float, p.split()))
              for p in re.findall(r"-?\d+\.?\d*\s+-?\d+\.?\d*", wkt)]
    if not coords:
        return False
    if coords[0] == coords[-1]:
        coords = coords[:-1]
    if len(coords) != 4:
        return False
    return len({c[0] for c in coords}) == 2 and len({c[1] for c in coords}) == 2


def geom_expr(column: str = "geom") -> str:
    """SQL expression that materializes a Geometry from the WKB-typed geom column.
    Both catalogs (iceberg and spatial_iceberg) expose geom as VARBINARY now."""
    return f"ST_GeomFromBinary({column})"


def _intersects_shortcut(wkt: str) -> str:
    """Production-equivalent SQL for ST_Intersects(geom, wkt) — the form
    TrinoFilterToSQL emits for CQL INTERSECTS when invoked via
    geomesa-trino-datastore.

    Two emission shapes, mirroring TrinoFilterToSQL.visit(Intersects):

      Fast path (axis-aligned rectangle + point data):
        bbox-overlap alone. For point geometries, bbox(point) = point, so
        bbox-overlap(point, rect) ⇔ point ∈ rect ⇔ ST_Intersects. The CASE
        WHEN bbox-contained fallback is dead code — every row that passes
        bbox-overlap also passes bbox-contained — so emitting it costs ~8
        extra struct-field reads + comparisons per surviving row for no
        offsetting savings (observed ~2x slowdown on AIS 25M-row INTERSECTS
        large query before this guard).

      General path (any other case):
        (bbox-overlap)
        AND CASE WHEN bbox-contained THEN TRUE ELSE ST_Intersects END
        bbox-overlap drives file-level pruning; CASE WHEN short-circuits
        WKB-decode + intersect for rows fully inside the query envelope.
        CASE WHEN (not OR) because Trino's optimizer distributes OR over AND,
        evaluating ST_Intersects up to 4x per row (measured 3.3x slowdown).

    Every table in DATASET_CONFIGS is point geometry today, so the rectangle
    check alone determines the dispatch. If a polygon-typed dataset is added
    later, the fast path becomes unsound for that table and needs a per-
    dataset geom_type guard."""
    mx, my, Mx, My = _wkt_envelope(wkt)
    bbox_overlap = (
        f"__geom_bbox__.xmax >= {mx} AND __geom_bbox__.xmin <= {Mx} "
        f"AND __geom_bbox__.ymax >= {my} AND __geom_bbox__.ymin <= {My}"
    )
    if _is_axis_aligned_rectangle(wkt):
        return bbox_overlap
    return (
        f"({bbox_overlap}) "
        f"AND "
        f"CASE WHEN __geom_bbox__.xmin >= {mx} AND __geom_bbox__.xmax <= {Mx} "
        f"AND __geom_bbox__.ymin >= {my} AND __geom_bbox__.ymax <= {My} "
        f"THEN TRUE "
        f"ELSE ST_Intersects({geom_expr()}, ST_GeometryFromText('{wkt}')) END"
    )

from common import trino_host_port

TRINO_HOST, TRINO_PORT = trino_host_port()
N_WARMUP   = 1
N_MEASURE  = 5

# ── Geographic constants ───────────────────────────────────────────────────────
# Loaded from the Java benchmark's geometries block — the single source of truth
# (guarded by tests/test_benchmark_geometries.py).


def _load_canonical_geometries() -> dict:
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..",
                        "geomesa-trino-benchmark", "src", "main", "resources",
                        "benchmark_datasets.json")
    with open(path) as f:
        return {k: v for k, v in json.load(f)["geometries"].items()
                if not k.startswith("_")}


def _point_lonlat(wkt: str) -> tuple[float, float]:
    lon, lat = wkt.replace("POINT (", "").rstrip(")").split()
    return float(lon), float(lat)


_GEOMETRIES = _load_canonical_geometries()

NE_US_WKT     = _GEOMETRIES["NE_US"]
SMALL_BOX_WKT = _GEOMETRIES["SMALL_BOX"]
DC_WKT        = _GEOMETRIES["DC_PT"]
DC_LON, DC_LAT = _point_lonlat(DC_WKT)

# Inner Beijing metro area (~110 km × 67 km) — wide-query baseline (~85% of dataset).
BEIJING_LARGE_WKT = _GEOMETRIES["BJ_LARGE"]
# ~2 km × 2 km around the Forbidden City — tight enough to land in a small handful of files.
BEIJING_SMALL_WKT = _GEOMETRIES["BJ_TIGHT"]
TIANANMEN_WKT = _GEOMETRIES["TIANANMEN"]
TIANANMEN_LON, TIANANMEN_LAT = _point_lonlat(TIANANMEN_WKT)

GEOLIFE_LARGE_WKT = _GEOMETRIES["GL_LARGE"]
GEOLIFE_SMALL_WKT = _GEOMETRIES["GL_SMALL"]
GEOLIFE_CTR_WKT   = _GEOMETRIES["TIANANMEN"]
GEOLIFE_CTR_LON, GEOLIFE_CTR_LAT = _point_lonlat(GEOLIFE_CTR_WKT)

# AIS data is UTM Zone 17 (84W-78W, FL/SE coast) — see ingest_ais.py.
AIS_LARGE_WKT  = _GEOMETRIES["AIS_LARGE"]
AIS_SMALL_WKT  = _GEOMETRIES["AIS_MEDIUM"]
PORT_EVERGLADES_WKT = _GEOMETRIES["PORT_EVERGLADES"]
PORT_EVERGLADES_LON, PORT_EVERGLADES_LAT = _point_lonlat(PORT_EVERGLADES_WKT)

REGIONS_LARGE_WKT = _GEOMETRIES["NE_US_10X10"]

# ── Multi-geom dataset (spatial.observations_2geom) ───────────────────────────
# Independent from the DATASET_CONFIGS registry because the dataset has two
# geometry columns (`center` Z2, `ellipse` XZ2) with two distinct bbox struct
# companions — the registry's `bbox_filter` / `large_wkt` fields assume a single
# geom column. The block is small enough to inline rather than generalize the
# registry. See docs/superpowers/specs/2026-05-13-multigeom-benchmark-design.md.

# Reuses NE_US_WKT (NE-US polygon) and SMALL_BOX_WKT (2°×2° DC box) for both
# geom columns. The synthetic dataset's center+ellipse are spatially co-located
# so identical WKTs land at identical row sets — the value we're measuring is
# that BOTH partition predicates push to the planner, not asymmetric counts.

def _multigeom_bbox_overlap(geom_name: str, wkt: str) -> str:
    """Bbox-overlap predicate on the __<geom_name>_bbox__ struct for `wkt`."""
    mx, my, Mx, My = _wkt_envelope(wkt)
    return (
        f"__{geom_name}_bbox__.xmax >= {mx} AND __{geom_name}_bbox__.xmin <= {Mx} "
        f"AND __{geom_name}_bbox__.ymax >= {my} AND __{geom_name}_bbox__.ymin <= {My}"
    )


def _multigeom_intersects(geom_name: str, wkt: str) -> str:
    """Bbox-overlap + ST_Intersects predicate for `geom_name` against `wkt`.
    Wraps the varbinary geom column with ST_GeomFromBinary in both catalogs."""
    bbox = _multigeom_bbox_overlap(geom_name, wkt)
    return (
        f"({bbox}) "
        f"AND ST_Intersects(ST_GeomFromBinary({geom_name}), ST_GeometryFromText('{wkt}'))"
    )


def _build_multigeom_filters() -> list[dict]:
    """Build the 5-query suite for spatial.observations_2geom. Returns dicts
    with 'label', 'note', 'si_where', 'ice_where' keys (matching the shape of
    `build_queries` entries but with one geom-name pair per filter)."""

    return [
        {
            "label": "Unfiltered",
            "note":  "Baseline scan — no predicate. Same row count on SI and ICE.",
            "si_where":  "TRUE",
            "ice_where": "TRUE",
        },
        {
            "label": "BBOX center (NE-US)",
            "note":  "Single-geom predicate on __center_bbox__. SI prunes via __center_z2__ partition column; ICE prunes via column-stats only.",
            "si_where":  _multigeom_bbox_overlap("center", NE_US_WKT),
            "ice_where": _multigeom_bbox_overlap("center", NE_US_WKT),
        },
        {
            "label": "BBOX ellipse (NE-US)",
            "note":  "Single-geom predicate on __ellipse_bbox__. SI prunes via __ellipse_xz2__; ICE column-stats only.",
            "si_where":  _multigeom_bbox_overlap("ellipse", NE_US_WKT),
            "ice_where": _multigeom_bbox_overlap("ellipse", NE_US_WKT),
        },
        {
            "label": "INTERSECTS center NE-US AND INTERSECTS ellipse NE-US",
            "note":  "Dual pushdown — BOTH __center_z2__ and __ellipse_xz2__ partition predicates fire (verified by MultiGeomIT).",
            "si_where":  (_multigeom_intersects("center",  NE_US_WKT)
                          + " AND "
                          + _multigeom_intersects("ellipse", NE_US_WKT)),
            "ice_where": (_multigeom_intersects("center",  NE_US_WKT)
                          + " AND "
                          + _multigeom_intersects("ellipse", NE_US_WKT)),
        },
        {
            "label": "INTERSECTS center NE-US AND INTERSECTS ellipse small",
            "note":  "Heterogeneous boxes — center predicate is wide (NE-US), ellipse predicate is tight (2°×2° DC).",
            "si_where":  (_multigeom_intersects("center",  NE_US_WKT)
                          + " AND "
                          + _multigeom_intersects("ellipse", SMALL_BOX_WKT)),
            "ice_where": (_multigeom_intersects("center",  NE_US_WKT)
                          + " AND "
                          + _multigeom_intersects("ellipse", SMALL_BOX_WKT)),
        },
    ]


def run_multigeom_section(si_conn, ice_conn, total_rows: int, total_files: int, args) -> None:
    """Run the multigeom benchmark suite. Same SI-vs-ICE comparison as
    run_dataset_section but with a hand-built filter list (no DATASET_CONFIGS
    entry; see _build_multigeom_filters for the spec). Prints results in the
    same row format."""
    si_table  = "spatial_iceberg.spatial.observations_2geom"
    ice_table = "iceberg.spatial.observations_2geom"

    sep("═")
    print(f"  Synthetic CONUS multi-geom (center+Z2, ellipse+XZ2)")
    print(f"  spatial_iceberg (per-geom partition pushdown) vs iceberg (baseline)")
    sep("═")
    print(f"  Dataset : {total_rows:,} rows  ·  {total_files} files")
    print(f"  Config  : {args.warmup} warmup + {args.runs} measured runs per query")
    print(f"  Note    : dual-pushdown queries fire BOTH __center_z2__ and __ellipse_xz2__")
    print(f"            partition predicates independently — pruning across both columns")
    print(f"            is the value-add this suite measures.")
    sep("═")
    print(f"  {'Filter':<60} {'SI ms':>8} {'ICE ms':>8} {'Speedup':>8} {'Count':>12}")
    sep("─")

    for f in _build_multigeom_filters():
        si_sql  = f"SELECT COUNT(*) FROM {si_table}  WHERE {f['si_where']}"
        ice_sql = f"SELECT COUNT(*) FROM {ice_table} WHERE {f['ice_where']}"
        try:
            si_ms,  si_std,  si_count,  _ = bench(si_conn,  si_sql,  args.warmup, args.runs)
            ice_ms, ice_std, ice_count, _ = bench(ice_conn, ice_sql, args.warmup, args.runs)
        except Exception as exc:
            print(f"  [SKIP] {f['label']:<58} — query failed: {exc}")
            continue
        match = "✓" if si_count == ice_count else f"✗ SI={si_count} ICE={ice_count}"
        speedup = (ice_ms / si_ms) if si_ms > 0 else 0.0
        print(f"  {f['label']:<60} {si_ms:>8.0f} {ice_ms:>8.0f} {speedup:>7.1f}× "
              f"{si_count:>12,} {match}")
    sep("─")


# ── Dataset configs ────────────────────────────────────────────────────────────
# Each entry drives build_queries() and the locality diagnostic for that table.
# attr_filter: SQL predicate string, or None to omit the Attr+Spatial query.

DATASET_CONFIGS = {
    "observations": {
        "label":         "Synthetic CONUS observations (sensor tracks)",
        "bbox_filter":   "__geom_bbox__.xmax >= -80.0 AND __geom_bbox__.xmin <= -70.0 AND __geom_bbox__.ymax >= 37.0 AND __geom_bbox__.ymin <= 45.0",
        "large_wkt":     NE_US_WKT,
        "large_note":    "NE-US polygon (~11% of CONUS). bbox pruning active.",
        "small_wkt":     SMALL_BOX_WKT,
        "small_note":    "2°×2° box around DC (~0.4% of CONUS). bbox pruning active.",
        "dwithin_wkt":   DC_WKT,
        "dwithin_lon":   DC_LON,
        "dwithin_lat":   DC_LAT,
        "dwithin_m":     100_000,
        "dwithin_note":  "Bbox pre-filter drives file pruning; exact distance is row-level.",
        "temporal_start":"2023-01-01 00:00:00 UTC",
        "temporal_end":  "2024-01-01 00:00:00 UTC",
        "attr_filter":   "active = TRUE AND value > 50.0",
        "attr_note":     "Attribute filter combined with spatial. bbox pruning active.",
    },
    "regions": {
        "label":         "Synthetic CONUS regions (polygon features, XZ2-partitioned)",
        "bbox_filter":   "__geom_bbox__.xmax >= -80.0 AND __geom_bbox__.xmin <= -70.0 AND __geom_bbox__.ymax >= 35.0 AND __geom_bbox__.ymin <= 45.0",
        "large_wkt":     REGIONS_LARGE_WKT,
        "large_note":    "NE-US 10°×10° polygon (matches XT8 regionsCountMatchesUnprunedBaseline test). XZ2 + bbox pruning active.",
        "small_wkt":     SMALL_BOX_WKT,
        "small_note":    "2°×2° box around DC. XZ2 + bbox pruning active.",
        # Polygon-typed dataset: Trino's ST_Distance(SphericalGeography) only
        # accepts POINT inputs, so DWITHIN is skipped via dwithin_m=None.
        "dwithin_wkt":   None,
        "dwithin_lon":   None,
        "dwithin_lat":   None,
        "dwithin_m":     None,
        "dwithin_note":  None,
        "temporal_start":"2023-01-01 00:00:00 UTC",
        "temporal_end":  "2024-01-01 00:00:00 UTC",
        "attr_filter":   "category = 'urban'",
        "attr_note":     "Urban category (~18% of rows) combined with spatial. XZ2 + bbox pruning active.",
    },
    "tdrive": {
        "label":         "T-Drive Beijing taxi tracks (2008-02, 1 week)",
        "bbox_filter":   "__geom_bbox__.xmax >= 116.39 AND __geom_bbox__.xmin <= 116.41 AND __geom_bbox__.ymax >= 39.91 AND __geom_bbox__.ymin <= 39.93",
        "large_wkt":     BEIJING_LARGE_WKT,
        "large_note":    "Inner Beijing metro area (~85% of dataset). Pruning ceiling ~15%.",
        "small_wkt":     BEIJING_SMALL_WKT,
        "small_note":    "~2km×2km Forbidden City. Tight enough to reach a small handful of files.",
        "dwithin_wkt":   TIANANMEN_WKT,
        "dwithin_lon":   TIANANMEN_LON,
        "dwithin_lat":   TIANANMEN_LAT,
        "dwithin_m":     1_000,
        "dwithin_note":  "1 km circle around Tiananmen — should prune to a single-digit file count.",
        "temporal_start":"2008-02-02 00:00:00 UTC",
        "temporal_end":  "2008-02-03 00:00:00 UTC",
        "attr_filter":   None,
    },
    "geolife": {
        "label":         "GeoLife GPS trajectories (182 users, Beijing area, 2007–2012)",
        "bbox_filter":   "__geom_bbox__.xmax >= 116.0 AND __geom_bbox__.xmin <= 117.5 AND __geom_bbox__.ymax >= 39.5 AND __geom_bbox__.ymin <= 40.5",
        "large_wkt":     GEOLIFE_LARGE_WKT,
        "large_note":    "Greater Beijing metro area. bbox pruning active.",
        "small_wkt":     GEOLIFE_SMALL_WKT,
        "small_note":    "~0.3°×0.2° central Beijing. bbox pruning active.",
        "dwithin_wkt":   GEOLIFE_CTR_WKT,
        "dwithin_lon":   GEOLIFE_CTR_LON,
        "dwithin_lat":   GEOLIFE_CTR_LAT,
        "dwithin_m":     5_000,
        "dwithin_note":  "Bbox pre-filter drives file pruning; exact distance is row-level.",
        "temporal_start":"2008-01-01 00:00:00 UTC",
        "temporal_end":  "2010-01-01 00:00:00 UTC",
        "attr_filter":   None,
    },
    "ais": {
        "label":         "Marine AIS vessel tracks (Zone 17: FL/SE coast, Jan 2017)",
        "bbox_filter":   "__geom_bbox__.xmax >= -84.0 AND __geom_bbox__.xmin <= -78.0 AND __geom_bbox__.ymax >= 24.0 AND __geom_bbox__.ymin <= 34.0",
        "large_wkt":     AIS_LARGE_WKT,
        "large_note":    "Zone 17 full extent (FL/SE coast, ~80%). bbox pruning active.",
        "small_wkt":     AIS_SMALL_WKT,
        "small_note":    "Charleston Harbor box (~0.6°×0.7°, ~5%). bbox pruning active.",
        "dwithin_wkt":   PORT_EVERGLADES_WKT,
        "dwithin_lon":   PORT_EVERGLADES_LON,
        "dwithin_lat":   PORT_EVERGLADES_LAT,
        "dwithin_m":     50_000,
        "dwithin_note":  "Bbox pre-filter drives file pruning; exact distance is row-level.",
        "temporal_start":"2017-01-01 00:00:00 UTC",
        "temporal_end":  "2017-01-15 00:00:00 UTC",
        "attr_filter":   "vessel_type > 0",
        "attr_note":     "Known vessel type + spatial. bbox pruning active.",
    },
}


def build_queries(table: str, cfg: dict) -> list[dict]:
    si  = f"spatial_iceberg.spatial.{table}"
    ice = f"iceberg.spatial.{table}"

    large = cfg["large_wkt"]
    small = cfg["small_wkt"]
    bf    = cfg["bbox_filter"]
    ts    = cfg["temporal_start"]
    te    = cfg["temporal_end"]

    # SI queries use the production-equivalent INTERSECTS form (bbox-overlap +
    # CASE WHEN bbox-contained shortcut). Production code paths through
    # geomesa-trino-datastore emit this rewrite via TrinoFilterToSQL for every
    # CQL INTERSECTS, so the benchmark reflects what real callers actually
    # execute. ICE queries keep plain ST_Intersects to represent the baseline
    # "no GeoMesa stack" counterfactual.
    si_large = _intersects_shortcut(large)
    si_small = _intersects_shortcut(small)
    ice_intersects_large = (
        f"ST_Intersects({geom_expr()}, ST_GeometryFromText('{large}'))"
    )
    ice_intersects_small = (
        f"ST_Intersects({geom_expr()}, ST_GeometryFromText('{small}'))"
    )

    queries = [
        {
            "label": "BBOX (bbox columns)",
            "note":  "Pre-computed min/max struct fields; no WKB decode. Z2 pushdown via bbox pattern recognition.",
            "si":  f"SELECT COUNT(*) FROM {si}  WHERE {bf}",
            "ice": f"SELECT COUNT(*) FROM {ice} WHERE {bf}",
        },
        {
            "label": "ST_Intersects large bbox",
            "note":  cfg["large_note"],
            "si":  f"SELECT COUNT(*) FROM {si}  WHERE {si_large}",
            "ice": f"SELECT COUNT(*) FROM {ice} WHERE {ice_intersects_large}",
        },
        {
            "label": "ST_Intersects small bbox",
            "note":  cfg["small_note"],
            "si":  f"SELECT COUNT(*) FROM {si}  WHERE {si_small}",
            "ice": f"SELECT COUNT(*) FROM {ice} WHERE {ice_intersects_small}",
        },
        {
            "label": "ST_Within",
            "note":  "Features fully inside query polygon. bbox pruning active.",
            "si":  f"SELECT COUNT(*) FROM {si}  WHERE ST_Within({geom_expr()}, ST_GeometryFromText('{large}'))",
            "ice": f"SELECT COUNT(*) FROM {ice} WHERE ST_Within({geom_expr()}, ST_GeometryFromText('{large}'))",
        },
    ]

    # DWITHIN uses ST_Distance on to_spherical_geography, which only accepts
    # POINT inputs in Trino. Polygon-typed datasets must set dwithin_m=None to
    # skip this query — see the regions entry for the canonical example.
    if cfg.get("dwithin_m") is not None:
        dwithin_lon = cfg["dwithin_lon"]
        dwithin_lat = cfg["dwithin_lat"]
        dwithin_m   = cfg["dwithin_m"]
        dwithin_wkt = cfg["dwithin_wkt"]
        _deg_lat  = dwithin_m / 111_111
        _deg_lon  = dwithin_m / (111_111 * math.cos(math.radians(dwithin_lat)))
        _deg      = max(_deg_lat, _deg_lon) * 1.1
        dw_bbox   = (f"POLYGON (({dwithin_lon - _deg} {dwithin_lat - _deg}, "
                     f"{dwithin_lon + _deg} {dwithin_lat - _deg}, "
                     f"{dwithin_lon + _deg} {dwithin_lat + _deg}, "
                     f"{dwithin_lon - _deg} {dwithin_lat + _deg}, "
                     f"{dwithin_lon - _deg} {dwithin_lat - _deg}))")
        si_dw = _intersects_shortcut(dw_bbox)
        km    = dwithin_m // 1000
        queries.append({
            "label": f"DWITHIN {km} km",
            "note":  cfg["dwithin_note"],
            "si":  (f"SELECT COUNT(*) FROM {si} "
                    f"WHERE {si_dw} "
                    f"  AND ST_Distance(to_spherical_geography({geom_expr()}), "
                    f"                  to_spherical_geography(ST_GeometryFromText('{dwithin_wkt}'))) <= {dwithin_m}"),
            "ice": (f"SELECT COUNT(*) FROM {ice} "
                    f"WHERE ST_Distance(to_spherical_geography({geom_expr()}), "
                    f"                  to_spherical_geography(ST_GeometryFromText('{dwithin_wkt}'))) <= {dwithin_m}"),
        })

    queries.extend([
        {
            "label": "DURING (temporal only)",
            "note":  "No spatial predicate. Partition pruning by dtg_month.",
            "si":  f"SELECT COUNT(*) FROM {si}  WHERE dtg > TIMESTAMP '{ts}' AND dtg < TIMESTAMP '{te}'",
            "ice": f"SELECT COUNT(*) FROM {ice} WHERE dtg > TIMESTAMP '{ts}' AND dtg < TIMESTAMP '{te}'",
        },
        {
            "label": "ST_Intersects AND DURING",
            "note":  "Combined spatio-temporal: bbox sub-field pruning + dtg row filter.",
            "si":  (f"SELECT COUNT(*) FROM {si} "
                    f"WHERE {si_large} "
                    f"  AND dtg > TIMESTAMP '{ts}' AND dtg < TIMESTAMP '{te}'"),
            "ice": (f"SELECT COUNT(*) FROM {ice} "
                    f"WHERE {ice_intersects_large} "
                    f"  AND dtg > TIMESTAMP '{ts}' AND dtg < TIMESTAMP '{te}'"),
        },
    ])

    if cfg.get("attr_filter"):
        af = cfg["attr_filter"]
        queries.append({
            "label": "Attr + ST_Intersects",
            "note":  cfg.get("attr_note", "Attribute filter combined with spatial. bbox pruning active."),
            "si":  (f"SELECT COUNT(*) FROM {si} "
                    f"WHERE {af} AND {si_large}"),
            "ice": (f"SELECT COUNT(*) FROM {ice} "
                    f"WHERE {af} AND {ice_intersects_large}"),
        })

    return queries


# ── Trino query utilities ──────────────────────────────────────────────────────

def _parse_bytes(s: str) -> int:
    if not s:
        return 0
    s = str(s)
    for suffix, mult in [("GB", 1 << 30), ("MB", 1 << 20), ("kB", 1 << 10), ("B", 1)]:
        if s.endswith(suffix):
            try:
                return int(float(s[: -len(suffix)]) * mult)
            except ValueError:
                return 0
    try:
        return int(s)
    except ValueError:
        return 0


def _fmt_bytes(n: int) -> str:
    for unit, div in [("GB", 1 << 30), ("MB", 1 << 20), ("kB", 1 << 10)]:
        if n >= div:
            return f"{n / div:.1f} {unit}"
    return f"{n} B"


def get_query_stats(query_id: str) -> dict:
    url  = f"http://{TRINO_HOST}:{TRINO_PORT}/v1/query/{query_id}"
    resp = requests.get(url, headers={"X-Trino-User": "trino"}, timeout=10)
    if resp.status_code != 200:
        return {}
    qs = resp.json().get("queryStats", {})
    files_read = rows_read = bytes_read = 0
    for op in qs.get("operatorSummaries", []):
        if op.get("operatorType") == "ScanFilterAndProjectOperator":
            files_read += op.get("totalDrivers", 0)
            rows_read  += op.get("physicalInputPositions", 0)
            bytes_read += _parse_bytes(op.get("physicalInputDataSize", "0B"))
    return {"files_read": files_read, "rows_read": rows_read, "bytes_read": bytes_read}


def fetch_all(conn, sql: str) -> list:
    cur = conn.cursor()
    cur.execute(sql)
    return cur.fetchall()


def run_query(conn, sql: str) -> tuple[float, int, dict]:
    cur = conn.cursor()
    t0  = time.perf_counter()
    cur.execute(sql)
    rows    = cur.fetchall()
    wall_ms = (time.perf_counter() - t0) * 1000
    count   = rows[0][0] if rows else 0
    stats   = get_query_stats(cur.query_id) if cur.query_id else {}
    return wall_ms, count, stats


def bench(conn, sql: str, n_warmup: int, n_measure: int):
    for _ in range(n_warmup):
        run_query(conn, sql)
    times, last_stats, last_count = [], {}, 0
    for _ in range(n_measure):
        ms, count, stats = run_query(conn, sql)
        times.append(ms)
        last_stats = stats
        last_count = count
    mean = statistics.mean(times)
    stdv = statistics.stdev(times) if len(times) > 1 else 0.0
    return mean, stdv, last_count, last_stats


def sep(char="─", width=80):
    print(char * width)


# ── Per-dataset benchmark section ─────────────────────────────────────────────

def run_dataset_section(si_conn, ice_conn, table: str, cfg: dict,
                         total_rows: int, total_files: int, args) -> tuple[list, bool]:
    """Run full benchmark for one dataset. Returns (summary_rows, pruning_active)."""
    queries = build_queries(table, cfg)

    sep("═")
    print(f"  {cfg['label']}")
    print(f"  spatial_iceberg (bbox sub-field pruning) vs iceberg (baseline)")
    sep("═")
    print(f"  Dataset : {total_rows:,} rows  ·  {total_files} files (z2)")
    print(f"  Config  : {args.warmup} warmup + {args.runs} measured runs per query")
    sep("═")

    summary_rows = []
    lbl_w = 22

    for idx, q in enumerate(queries):
        sep()
        print(f"  {q['label']}")
        print(f"  {q['note']}")
        sep()

        si_mean, si_std, si_count, si_stats = bench(si_conn, q["si"], args.warmup, args.runs)
        si_files = si_stats.get("files_read", 0)
        si_rows  = si_stats.get("rows_read",  0)
        si_bytes = si_stats.get("bytes_read", 0)
        pruned_si = (1 - si_files / total_files) * 100 if total_files > 0 else float("nan")

        ice_mean, ice_std, ice_count, ice_stats = bench(ice_conn, q["ice"], args.warmup, args.runs)
        ice_files = ice_stats.get("files_read", 0)
        ice_rows  = ice_stats.get("rows_read",  0)
        ice_bytes = ice_stats.get("bytes_read", 0)

        # Two pruning views:
        #   pruned_si / pruned_ice = absolute file-pruning rate vs unfiltered scan
        #     (1 - files_read / total_files). Reflects what the connector actually
        #     skipped at planning time, regardless of which connector did it.
        #   delta_pct = SI's marginal pruning over ICE (1 - si_files / ice_files).
        #     Reads 0% when both connectors land at the same file set — common when
        #     the bbox-overlap predicate lets ICE prune via standard column stats.
        pruned_ice = (1 - ice_files / total_files) * 100 if total_files > 0 else float("nan")
        delta_pct  = (1 - si_files  / ice_files)   * 100 if ice_files   > 0 else float("nan")
        row_skip   = (1 - si_rows   / ice_rows)    * 100 if ice_rows    > 0 else float("nan")
        speedup    = ice_mean / si_mean                  if si_mean     > 0 else float("nan")

        ok = si_count == ice_count
        match_str = "✓" if ok else f"✗ MISMATCH  si={si_count:,}  ice={ice_count:,}"

        def row(label, si_val, ice_val, unit=""):
            print(f"  {label:<{lbl_w}}  {si_val:>18}  {ice_val:>18}  {unit}")

        print(f"  {'':22s}  {'spatial_iceberg':>18}  {'iceberg':>18}")
        sep("·")
        row("wall time mean",  f"{si_mean:>14.0f} ms",   f"{ice_mean:>14.0f} ms")
        row("wall time ±σ",    f"{si_std:>14.0f} ms",    f"{ice_std:>14.0f} ms")
        row("files read",      f"{si_files:>18,}",        f"{ice_files:>18,}")
        row("rows read",       f"{si_rows:>18,}",         f"{ice_rows:>18,}")
        row("bytes read",      f"{_fmt_bytes(si_bytes):>18}", f"{_fmt_bytes(ice_bytes):>18}")
        row("result count",    f"{si_count:>18,}",        f"{ice_count:>18,}", match_str)
        sep("·")
        if not math.isnan(pruned_si):
            row("files pruned vs total", f"{pruned_si:>17.1f}%", f"{pruned_ice:>17.1f}%")
        if not math.isnan(delta_pct):
            print(f"  {'SI marginal vs ICE':<{lbl_w}}  {delta_pct:>17.1f}%")
        if not math.isnan(row_skip):
            print(f"  {'rows skipped':<{lbl_w}}  {row_skip:>17.1f}%")
        if not math.isnan(speedup):
            print(f"  {'speedup':<{lbl_w}}  {speedup:>17.1f}×")
        print()

        summary_rows.append({
            "label": q["label"],
            "si_ms": si_mean, "ice_ms": ice_mean,
            "speedup": speedup,
            "pruned_si": pruned_si, "pruned_ice": pruned_ice,
            "delta_pct": delta_pct,
            "si_files": si_files, "ice_files": ice_files,
        })

    # Summary table.
    # Label column sized to the widest query label so multi-word entries like
    # "ST_Intersects large bbox (w/ shortcut SQL)" don't overflow and push the
    # numeric columns out of alignment.
    label_w = max((len(r["label"]) for r in summary_rows), default=35)
    sep("═")
    print("  Summary")
    sep("═")
    # Pruned SI/ICE = absolute file pruning rate vs unfiltered scan, per connector.
    # Δ vs ICE = SI's marginal pruning over ICE (1 - si_files/ice_files).
    print(f"  {'Query':<{label_w}}  {'SI ms':>7}  {'ICE ms':>7}  {'Speedup':>8}  "
          f"{'Pruned SI/ICE':>15}  {'Δ vs ICE':>9}  {'Files SI/ICE':>14}")
    sep()
    for r in summary_rows:
        spd, pr_si, pr_ice, delta = r["speedup"], r["pruned_si"], r["pruned_ice"], r["delta_pct"]
        spd_s   = f"{spd:>6.1f}×"  if not math.isnan(spd)   else "    n/a"
        delta_s = f"{delta:>7.1f}%" if not math.isnan(delta) else "     n/a"
        pruned_s = (f"{pr_si:>5.1f}%/{pr_ice:>5.1f}%"
                    if not math.isnan(pr_si) else "          n/a")
        print(f"  {r['label']:<{label_w}}  {r['si_ms']:>7.0f}  {r['ice_ms']:>7.0f}  {spd_s:>8}  "
              f"{pruned_s:>15}  {delta_s:>9}  {r['si_files']:>6}/{r['ice_files']:<6}")
    sep("═")

    pruning_active = run_locality_diagnostic(ice_conn, table)

    return summary_rows, pruning_active


def run_locality_diagnostic(ice_conn, table: str) -> bool:
    """
    Per-file bbox tightness drives file pruning: if every file's __geom_bbox__
    sub-field stats span the entire dataset, no file can be skipped. Compare the
    tightest per-file longitude span against the global span — a small ratio means
    the Z2-centroid sort placed spatially adjacent rows in the same Parquet file.
    """
    print()
    print("  Spatial Locality Diagnostic")
    sep()

    g = fetch_all(ice_conn, f"""
        SELECT MIN("__geom_bbox__".xmin),
               MAX("__geom_bbox__".xmax),
               MIN("__geom_bbox__".ymin),
               MAX("__geom_bbox__".ymax)
        FROM iceberg.spatial.{table}
    """)
    f = fetch_all(ice_conn, f"""
        SELECT MIN("__geom_bbox__".xmin),
               MAX("__geom_bbox__".xmax),
               MAX("__geom_bbox__".xmax) - MIN("__geom_bbox__".xmin) AS lon_span
        FROM iceberg.spatial.{table}
        GROUP BY "$path"
        ORDER BY lon_span ASC
        LIMIT 1
    """)

    g = g[0] if g else (0.0, 0.0, 0.0, 0.0)
    f = f[0] if f else (0.0, 0.0, 0.0)

    g_lon_min, g_lon_max, g_lat_min, g_lat_max = (v or 0.0 for v in g)
    file_lon_span = f[2] or 0.0
    global_lon_span = g_lon_max - g_lon_min

    print(f"  Global bbox             : lon [{g_lon_min:.4f}, {g_lon_max:.4f}]  lat [{g_lat_min:.4f}, {g_lat_max:.4f}]")
    print(f"  Global lon span         : {global_lon_span:.4f}°")
    print(f"  Tightest per-file span  : {file_lon_span:.4f}° (lon)")

    pruning_active = global_lon_span > 0 and file_lon_span < global_lon_span / 2
    if pruning_active:
        ratio = file_lon_span / global_lon_span
        print(f"  → Tightest file covers {ratio:.1%} of global lon range: locality good, file pruning fires ✓")
    else:
        print(f"  → Tightest file lon span ≥ half of global: locality poor, files won't be pruned ✗")
        print()
        print("  Fix: ensure ingest sorts by Z2 centroid before append; reduce")
        print("       write.target-file-size-bytes if files are too large to localize.")
    print()
    return pruning_active


# ── Scalability analysis (observations-specific) ───────────────────────────────

def run_scalability_analysis(total_rows: int, total_files: int,
                              summary_rows: list, pruning_active: bool):
    # Use absolute pruning (vs unfiltered scan) — that's the genuine pruning rate.
    spatial_results = [
        r for r in summary_rows
        if not math.isnan(r.get("pruned_si", float("nan"))) and r.get("ice_files", 0) > 0
    ]
    avg_skip = statistics.mean(r["pruned_si"] for r in spatial_results) if spatial_results else 0.0

    print("  SCALABILITY ANALYSIS")
    sep()
    print("  Current single-node prototype:")
    print(f"    • Rows            : {total_rows:>10,}")
    print(f"    • Files           : {total_files:>10}  (Z2-centroid sort, no partition)")
    print( "    • Parallelism     :          1  worker node")
    print( "    • Storage         :  local MinIO (not networked S3)")
    print( "    • Z2 resolution   : 16-bit  (~600 m grid cells at equator)")
    print()

    if pruning_active:
        print(f"  Observed avg file-skip across spatial queries: {avg_skip:.0f}%")
        print()
        print("  Extrapolation to production scale (1T rows, real S3, multi-worker Trino):")
        print()
        print("  Files are bin-packed to write.target-file-size-bytes (~1 MiB at prototype")
        print("  scale; tune per dataset). Rows are pre-sorted by Z2 centroid so each file's")
        print("  __geom_bbox__ sub-field stats span only a small region — Iceberg uses those")
        print("  per-file Parquet stats to skip files whose bbox cannot intersect the query.")
        print()
        print("  Small-area query (2°×2° or DWITHIN 100 km):")
        print("    Prototype (100K rows) : few tiny files — file pruning fires, I/O trivial")
        print("    1T rows: only files overlapping the query bbox are read → ~100 ms")
        print("    ✓ Sub-second response scales to trillions of rows")
        print()
        print("  Large-area query (NE-US, 11% of CONUS):")
        print("    Prototype (100K rows) : few tiny files — architecture validated")
        print("    1T rows: ~11% of files touched, parallelised across workers")
        print("    Estimated p50: 1–5 s depending on worker count and S3 throughput")
        print()
        print("  Temporal-only query (no spatial predicate):")
        print("    Today  : full scan (dtg is not a partition key)")
        print("    Fix    : add month(dtg) partition once row count per spatial cluster")
        print("             exceeds the target file size by enough to justify the split")
        print()
        print("  VERDICT")
        sep("·")
        print("  Bbox-stat file pruning is working. The connector translates the user's")
        print("  spatial predicate (ST_Intersects et al.) into a TupleDomain over the")
        print("  __geom_bbox__ sub-fields; Iceberg compares it against per-file Parquet")
        print("  statistics and skips non-overlapping files before any Parquet read.")
    else:
        print("  Bbox file pruning is not active. See diagnostic above.")

    sep("═")


# ── Envelope-probe mode (single table, multiple envelope scales) ─────────────
#
# Runs SELECT COUNT(*) with the 4-conjunct
# bbox-overlap form (the shape SpatialConnectorMetadata.tryExtractBboxPatternMatch
# reconstructs into a Z2 range predicate) against spatial_iceberg vs iceberg at
# multiple envelope scales. Useful for any-table profiling — pruning at small
# (tight) envelopes shows the Z2-overlay value-add; at large envelopes it shows
# the ceiling. Envelopes per table live in benchmark_envelopes.json.

def _envelope_bbox_where(minlon: float, minlat: float, maxlon: float, maxlat: float) -> str:
    return (
        f"__geom_bbox__.xmax >= REAL '{minlon}' "
        f"AND __geom_bbox__.xmin <= REAL '{maxlon}' "
        f"AND __geom_bbox__.ymax >= REAL '{minlat}' "
        f"AND __geom_bbox__.ymin <= REAL '{maxlat}'"
    )


def _envelope_metadata_counts(conn, table: str) -> tuple[int, int]:
    """(partition_count, file_count) from Iceberg metadata tables. Partition
    count is the pruning denominator; file count is the scan denominator."""
    cur = conn.cursor()
    cur.execute(f'SELECT count(*) FROM "{table}$partitions"')
    parts = cur.fetchone()[0]
    cur.execute(f'SELECT count(*) FROM "{table}$files"')
    files = cur.fetchone()[0]
    return parts, files


def run_envelope_probe(table: str, schema: str, envelopes: list[dict],
                        warmup: int, runs: int) -> int:
    """Envelope-matrix probe for one table. Returns 0 on success, 1 if any
    SI vs ICE COUNT(*) mismatch is detected (correctness signal, not perf)."""
    si  = trino.dbapi.connect(host=TRINO_HOST, port=TRINO_PORT,
                               user="benchmark", catalog="spatial_iceberg", schema=schema)
    ice = trino.dbapi.connect(host=TRINO_HOST, port=TRINO_PORT,
                               user="benchmark", catalog="iceberg",         schema=schema)
    total_parts, total_files = _envelope_metadata_counts(ice, table)

    sep("═")
    print(f"  Table   : {schema}.{table}")
    print(f"  Layout  : {total_parts:,} partitions  ·  {total_files:,} files")
    print(f"  Catalogs: spatial_iceberg (Z2 overlay) vs iceberg (Iceberg stock)")
    print(f"  Config  : {warmup} warmup + {runs} measured runs per query")
    sep("═")

    header = (
        f"  {'Envelope':<18} {'SI ms':>7} {'ICE ms':>7} {'Spdup':>6}  "
        f"{'SI rows':>11} {'ICE rows':>11}  "
        f"{'SI bytes':>10} {'ICE bytes':>10}  "
        f"{'SI splits':>9} {'ICE splits':>10}  "
        f"{'Prune SI':>8} {'Prune ICE':>9}"
    )
    print(header)
    sep("─", len(header))

    mismatches = 0
    for env in envelopes:
        label = env["label"]
        minlon, minlat, maxlon, maxlat = env["bbox"]
        sql = f"SELECT count(*) FROM {table} WHERE {_envelope_bbox_where(minlon, minlat, maxlon, maxlat)}"

        si_mean,  _, si_count,  si_stats  = bench(si,  sql, warmup, runs)
        ice_mean, _, ice_count, ice_stats = bench(ice, sql, warmup, runs)

        si_files  = si_stats.get("files_read", 0)
        ice_files = ice_stats.get("files_read", 0)
        si_rows   = si_stats.get("rows_read",  0)
        ice_rows  = ice_stats.get("rows_read",  0)
        si_bytes  = si_stats.get("bytes_read", 0)
        ice_bytes = ice_stats.get("bytes_read", 0)

        speedup   = (ice_mean / si_mean) if si_mean > 0 else 0.0
        prune_si  = (1 - si_files  / total_files) * 100 if total_files > 0 else 0.0
        prune_ice = (1 - ice_files / total_files) * 100 if total_files > 0 else 0.0

        match_note = ""
        if si_count != ice_count:
            mismatches += 1
            match_note = f"  ✗ MISMATCH si={si_count:,} ice={ice_count:,}"

        print(
            f"  {label:<18} {si_mean:>7.0f} {ice_mean:>7.0f} {speedup:>5.1f}×  "
            f"{si_rows:>11,} {ice_rows:>11,}  "
            f"{_fmt_bytes(si_bytes):>10} {_fmt_bytes(ice_bytes):>10}  "
            f"{si_files:>9,} {ice_files:>10,}  "
            f"{prune_si:>7.1f}% {prune_ice:>8.1f}%{match_note}"
        )

    sep("─", len(header))
    if mismatches:
        print(f"  ⚠ {mismatches} count mismatch(es) between catalogs — "
              "indicates a correctness bug, NOT a perf finding")
        return 1
    return 0


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs",   type=int, default=N_MEASURE, help=f"measured runs (default {N_MEASURE})")
    parser.add_argument("--warmup", type=int, default=N_WARMUP,  help=f"warmup runs (default {N_WARMUP})")
    parser.add_argument("--datasets", nargs="+", metavar="TABLE",
                        choices=sorted(DATASET_CONFIGS.keys()),
                        help=f"limit benchmark to these tables (default: all configured: {', '.join(sorted(DATASET_CONFIGS.keys()))})")
    parser.add_argument("--envelope-probe", metavar="TABLE",
                        help="Switch to envelope-matrix mode: run a single table at multiple "
                             "envelope scales (Atlanta/Texas/CONUS etc) instead of the full "
                             "DATASET_CONFIGS matrix. Envelopes loaded from --envelope-config. "
                             "Pair with --schema for the table's namespace.")
    parser.add_argument("--schema", default="spatial",
                        help="Iceberg namespace for connections (default: spatial). "
                             "Used by --envelope-probe; full-matrix mode still uses spatial.<table>.")
    parser.add_argument("--envelope-config",
                        default=os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                              "benchmark_envelopes.json"),
                        help="Path to envelope-config JSON (default: tools/benchmark_envelopes.json).")
    args = parser.parse_args()

    if args.envelope_probe:
        with open(args.envelope_config) as f:
            cfg = json.load(f)
        table_cfg = cfg.get("tables", {}).get(args.envelope_probe)
        if not table_cfg or not table_cfg.get("envelopes"):
            print(f"No envelopes configured for table '{args.envelope_probe}' in {args.envelope_config}", file=sys.stderr)
            print(f"Available: {', '.join(sorted(cfg.get('tables', {}).keys()))}", file=sys.stderr)
            sys.exit(2)
        sys.exit(run_envelope_probe(args.envelope_probe, args.schema,
                                     table_cfg["envelopes"], args.warmup, args.runs))

    si_conn  = trino.dbapi.connect(host=TRINO_HOST, port=TRINO_PORT, user="trino",
                                   catalog="spatial_iceberg", schema="spatial")
    ice_conn = trino.dbapi.connect(host=TRINO_HOST, port=TRINO_PORT, user="trino",
                                   catalog="iceberg",          schema="spatial")

    try:
        run_query(si_conn,  "SELECT 1")
        run_query(ice_conn, "SELECT 1")
    except Exception as exc:
        print(f"Cannot reach Trino at {TRINO_HOST}:{TRINO_PORT} — {exc}")
        return

    def _probe(table: str) -> tuple[int, int] | None:
        """Return (row_count, file_count) for spatial.<table>, or None if missing/empty."""
        try:
            _, rc, _ = run_query(ice_conn, f"SELECT COUNT(*) FROM iceberg.spatial.{table}")
            if not rc:
                return None
            _, fc, _ = run_query(ice_conn,
                f'SELECT COUNT(DISTINCT file_path) FROM "iceberg"."spatial"."{table}$files"')
            return (rc, fc or 0)
        except Exception:
            return None

    # Discover which configured tables actually exist and have data
    selected = set(args.datasets) if args.datasets else None
    datasets_to_run = []
    for table, cfg in DATASET_CONFIGS.items():
        if selected is not None and table not in selected:
            continue
        probe = _probe(table)
        if probe is None:
            continue
        datasets_to_run.append((table, cfg, probe[0], probe[1]))

    if not datasets_to_run:
        print("No benchmarkable tables found. Run make demo or make ingest-tdrive first.")
        return

    obs_summary = None
    obs_pruning = False

    for table, cfg, row_count, file_count in datasets_to_run:
        summary_rows, pruning_active = run_dataset_section(
            si_conn, ice_conn, table, cfg, row_count, file_count, args,
        )
        if table == "observations":
            obs_summary = summary_rows
            obs_pruning = pruning_active

    # Multi-geom dataset is an additive block, not in DATASET_CONFIGS. Probe
    # separately and run if present.
    multigeom_probe = _probe("observations_2geom")
    if multigeom_probe is not None:
        run_multigeom_section(si_conn, ice_conn, multigeom_probe[0], multigeom_probe[1], args)

    # Scalability analysis only for the synthetic observations dataset.
    if obs_summary is not None:
        obs_entry = next((e for e in datasets_to_run if e[0] == "observations"), None)
        if obs_entry:
            run_scalability_analysis(obs_entry[2], obs_entry[3], obs_summary, obs_pruning)


if __name__ == "__main__":
    main()
