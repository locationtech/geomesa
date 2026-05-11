import argparse
import sys
import uuid
import random
import collections
from datetime import datetime, timedelta, timezone

import pyarrow as pa
from shapely.geometry import Point, Polygon
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, BinaryType, BooleanType, DoubleType,
    TimestamptzType,
)
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError

sys.path.insert(0, ".")
import trino
from common import (
    trino_connect,
    GeometryColumn, TABLE_PROPERTIES, companion_fields, enable_pyiceberg_nested_metrics,
    geom_to_wkb, local_rest_catalog, metrics_properties_for, optimize_tables,
    partition_spec_for_geoms, with_companion_columns,
    cycle_visibility, visibilities_field,
)

enable_pyiceberg_nested_metrics()

# Synthetic CONUS data is uniform across a 58°×25° area. At Z2 bits=12 (total)
# the 64×64 global grid puts ~100 cells over CONUS — multiplied by 24 dtg_month
# partitions, that's ~2,400 tiny per-partition files for only 100k rows. bits=8
# (total) → 16×16 global → 22.5°×11.25° cells → ~9 CONUS cells × 24 months =
# ~216 partitions, so a BBOX query touching one cell × 24 months ≈ 24 files.
Z2_BITS = 8

# XZ2 needs a MUCH wider truncate than Z2. Z2 codes use the full 62-bit space, so their
# meaningful bits are the high (leading) hex chars that truncate-string keeps — bits=8 already
# discriminates. XZ2SFC(g=12) codes are tiny (≤ ~25 bits), hex-encoded with ~8 leading ZERO
# hex chars, so the signal is in the LOW chars. The connector skips XZ2 range pushdown unless
# the truncate width ≥ 13 hex chars (N ≥ 52 bits; MIN_XZ2_PRUNING_BITS in GeoMesaColumnCatalog) —
# below that, truncate buckets the whole table into one partition and it falls back to bbox-stat
# pruning (indistinguishable from the plain iceberg catalog). 52 -> ceil(52/4)=13 hex chars = the
# floor, the coarsest width that still lets SI prune by the xz2 column.
XZ2_BITS = 52

CATALOG = local_rest_catalog()

# Per-table geom descriptors. These drive schema construction, partition spec,
# table properties, and per-row companion-column population.
OBSERVATIONS_GEOMS = [GeometryColumn(name="geom", index="z2",  z2_bits=Z2_BITS)]
REGIONS_GEOMS      = [GeometryColumn(name="geom", index="xz2", z2_bits=XZ2_BITS)]
# Multi-geom proof dataset. 'center' is a point WKB (Z2-partitioned);
# 'ellipse' is a polygon WKB (XZ2-partitioned). Independent companion sets.
TWOGEOM_GEOMS = [
    GeometryColumn(name="center",  index="z2",  z2_bits=Z2_BITS),
    GeometryColumn(name="ellipse", index="xz2", z2_bits=XZ2_BITS),
]


def _observations_iceberg_schema(geoms: list[GeometryColumn]) -> Schema:
    base = [
        NestedField(1, "__fid__",        StringType(),    required=True),
        NestedField(2, "geom",           BinaryType()),
        NestedField(3, "dtg",            TimestamptzType()),
        NestedField(4, "sensor_id",      StringType()),
        NestedField(5, "value",          DoubleType()),
        NestedField(6, "active",         BooleanType()),
        visibilities_field(7),
    ]
    return Schema(*base, *companion_fields(geoms, next_id=8))


def _regions_iceberg_schema(geoms: list[GeometryColumn]) -> Schema:
    base = [
        NestedField(1, "__fid__",  StringType(),    required=True),
        NestedField(2, "geom",     BinaryType()),
        NestedField(3, "dtg",      TimestamptzType()),
        NestedField(4, "category", StringType()),
    ]
    return Schema(*base, *companion_fields(geoms, next_id=5))


def _twogeom_iceberg_schema(geoms: list[GeometryColumn]) -> Schema:
    base = [
        NestedField(1, "__fid__", StringType(),    required=True),
        NestedField(2, "center",  BinaryType()),
        NestedField(3, "ellipse", BinaryType()),
        NestedField(4, "dtg",     TimestamptzType()),
        NestedField(5, "label",   StringType()),
    ]
    return Schema(*base, *companion_fields(geoms, next_id=6))


US_MINX, US_MINY, US_MAXX, US_MAXY = -124.7, 24.5, -66.9, 49.4
START = datetime(2023, 1, 1, tzinfo=timezone.utc)
END   = datetime(2024, 12, 31, tzinfo=timezone.utc)
DATE_RANGE_S = int((END - START).total_seconds())
SENSOR_IDS  = [f"sensor-{i:04d}" for i in range(100)]
CATEGORIES  = ["urban", "rural", "forest", "water", "agricultural"]


def _rand_ts() -> datetime:
    return START + timedelta(seconds=random.randint(0, DATE_RANGE_S))


def setup_table(name: str, kind: str, geoms: list[GeometryColumn]):
    """Create spatial.<name> with the schema/partition spec for the given kind+geoms."""
    fq = f"spatial.{name}"
    try:
        CATALOG.create_namespace("spatial")
    except NamespaceAlreadyExistsError:
        pass
    try:
        CATALOG.drop_table(fq)
        print(f"Dropped existing {fq}")
    except NoSuchTableError:
        pass
    if kind == "observations":
        schema = _observations_iceberg_schema(geoms)
    elif kind == "twogeom":
        schema = _twogeom_iceberg_schema(geoms)
    else:
        schema = _regions_iceberg_schema(geoms)
    properties = {**TABLE_PROPERTIES, **metrics_properties_for(geoms)}
    tbl = CATALOG.create_table(
        fq,
        schema=schema,
        # Demo data spans 2 years × CONUS — day(dtg) gave ~6500 sparse partitions;
        # month(dtg) gives ~24 × ~9 Z2 cells at bits=8 ≈ 216 partitions.
        partition_spec=partition_spec_for_geoms(schema, geoms, dtg_grain="month"),
        properties=properties,
    )
    label = ",".join(f"{g.name}/{g.index or 'none'}" for g in geoms)
    print(f"Created {fq} (geoms: {label})")
    return tbl


def generate_observations(n: int) -> list[tuple]:
    """Generate n synthetic observation rows."""
    rows = []
    for i in range(n):
        lon = random.uniform(US_MINX, US_MAXX)
        lat = random.uniform(US_MINY, US_MAXY)
        pt  = Point(lon, lat)
        rows.append((
            str(uuid.uuid4()),
            geom_to_wkb(pt),
            _rand_ts(),
            random.choice(SENSOR_IDS),
            round(random.uniform(0.0, 100.0), 4),
            random.random() > 0.1,
            cycle_visibility(i),
        ))
    return rows


def generate_regions(n: int) -> list[tuple]:
    """Generate n region rows."""
    cols_per_side = int(n ** 0.5) + 1
    cell_w = (US_MAXX - US_MINX) / cols_per_side
    cell_h = (US_MAXY - US_MINY) / cols_per_side

    rows = []
    count = 0
    for i in range(cols_per_side):
        for j in range(cols_per_side):
            if count >= n:
                break
            x0, y0 = US_MINX + i * cell_w, US_MINY + j * cell_h
            x1, y1 = x0 + cell_w, y0 + cell_h
            poly = Polygon([(x0, y0), (x1, y0), (x1, y1), (x0, y1)])
            rows.append((
                str(uuid.uuid4()),
                geom_to_wkb(poly),
                _rand_ts(),
                random.choice(CATEGORIES),
            ))
            count += 1
        if count >= n:
            break
    return rows


def build_observations_pa(rows: list[tuple], geoms: list[GeometryColumn]) -> pa.Table:
    fids, wkbs, dtgs, sensors, values, actives, viss = [], [], [], [], [], [], []
    for fid, wkb, dtg, sensor_id, value, active, vis in rows:
        fids.append(fid); wkbs.append(wkb); dtgs.append(dtg)
        sensors.append(sensor_id); values.append(value); actives.append(active)
        viss.append(vis)
    base = pa.table({
        "__fid__":      pa.array(fids,    pa.string()),
        "geom":         pa.array(wkbs,    pa.large_binary()),
        "dtg":          pa.array(dtgs,    pa.timestamp("us", tz="UTC")),
        "sensor_id":    pa.array(sensors, pa.string()),
        "value":        pa.array(values,  pa.float64()),
        "active":       pa.array(actives, pa.bool_()),
        "visibilities": pa.array(viss,    pa.string()),
    }, schema=pa.schema([
        pa.field("__fid__",      pa.string(), nullable=False),
        pa.field("geom",         pa.large_binary()),
        pa.field("dtg",          pa.timestamp("us", tz="UTC")),
        pa.field("sensor_id",    pa.string()),
        pa.field("value",        pa.float64()),
        pa.field("active",       pa.bool_()),
        pa.field("visibilities", pa.string()),
    ]))
    return with_companion_columns(base, geoms)


def build_regions_pa(rows: list[tuple], geoms: list[GeometryColumn]) -> pa.Table:
    fids, wkbs, dtgs, cats = [], [], [], []
    for fid, wkb, dtg, category in rows:
        fids.append(fid); wkbs.append(wkb); dtgs.append(dtg); cats.append(category)
    base = pa.table({
        "__fid__":  pa.array(fids, pa.string()),
        "geom":     pa.array(wkbs, pa.large_binary()),
        "dtg":      pa.array(dtgs, pa.timestamp("us", tz="UTC")),
        "category": pa.array(cats, pa.string()),
    }, schema=pa.schema([
        pa.field("__fid__",  pa.string(), nullable=False),
        pa.field("geom",     pa.large_binary()),
        pa.field("dtg",      pa.timestamp("us", tz="UTC")),
        pa.field("category", pa.string()),
    ]))
    return with_companion_columns(base, geoms)


def generate_twogeom(n: int) -> list[tuple]:
    rows = []
    for _ in range(n):
        lon = random.uniform(US_MINX, US_MAXX)
        lat = random.uniform(US_MINY, US_MAXY)
        center = Point(lon, lat)
        # Small bbox-aligned ellipse around the center: half-width 0.05° lon, 0.03° lat.
        ellipse = Polygon([
            (lon - 0.05, lat - 0.03), (lon + 0.05, lat - 0.03),
            (lon + 0.05, lat + 0.03), (lon - 0.05, lat + 0.03),
        ])
        rows.append((
            str(uuid.uuid4()),
            geom_to_wkb(center),
            geom_to_wkb(ellipse),
            _rand_ts(),
            f"twogeom-{random.randint(0, 1000)}",
        ))
    return rows


def build_twogeom_pa(rows: list[tuple], geoms: list[GeometryColumn]) -> pa.Table:
    fids, centers, ellipses, dtgs, labels = [], [], [], [], []
    for fid, center, ellipse, dtg, label in rows:
        fids.append(fid); centers.append(center); ellipses.append(ellipse)
        dtgs.append(dtg); labels.append(label)
    base = pa.table({
        "__fid__": pa.array(fids,     pa.string()),
        "center":  pa.array(centers,  pa.large_binary()),
        "ellipse": pa.array(ellipses, pa.large_binary()),
        "dtg":     pa.array(dtgs,     pa.timestamp("us", tz="UTC")),
        "label":   pa.array(labels,   pa.string()),
    }, schema=pa.schema([
        pa.field("__fid__", pa.string(), nullable=False),
        pa.field("center",  pa.large_binary()),
        pa.field("ellipse", pa.large_binary()),
        pa.field("dtg",     pa.timestamp("us", tz="UTC")),
        pa.field("label",   pa.string()),
    ]))
    return with_companion_columns(base, geoms)


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--twogeom", action="store_true",
                        help="Also create spatial.observations_2geom — a multi-geom proof dataset "
                             "with center (Z2) and ellipse (XZ2) geometry columns.")
    args = parser.parse_args()

    obs_z2 = setup_table("observations", "observations", OBSERVATIONS_GEOMS)
    reg_z2 = setup_table("regions",      "regions",      REGIONS_GEOMS)
    if args.twogeom:
        twogeom = setup_table("observations_2geom", "twogeom", TWOGEOM_GEOMS)

    print("\nGenerating 100,000 observations...")
    obs_rows = generate_observations(100_000)
    obs_z2.append(build_observations_pa(obs_rows, OBSERVATIONS_GEOMS))
    print(f"  spatial.observations: {len(obs_rows):,} rows")
    year_dist = collections.Counter(r[2].year for r in obs_rows)
    for yr, cnt in sorted(year_dist.items()):
        print(f"  {yr}: {cnt:,} observations")

    print("\nGenerating 1,000 regions...")
    reg_rows = generate_regions(1_000)
    reg_z2.append(build_regions_pa(reg_rows, REGIONS_GEOMS))
    print(f"  spatial.regions: {len(reg_rows):,} rows")

    if args.twogeom:
        print("\nGenerating 10,000 twogeom rows...")
        rows = generate_twogeom(10_000)
        twogeom.append(build_twogeom_pa(rows, TWOGEOM_GEOMS))
        print(f"  spatial.observations_2geom: {len(rows):,} rows")

    print("\nCompacting tables (within-partition file fragments) ...")
    conn = trino_connect()
    names = ["observations", "regions"]
    if args.twogeom:
        names.append("observations_2geom")
    optimize_tables(conn, names)


if __name__ == "__main__":
    main()
