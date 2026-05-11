"""
Download, setup, and ingest T-Drive Beijing taxi GPS data into spatial.tdrive.

T-Drive: ~15M GPS points from 10,357 Beijing taxis, one week in February 2008.
  - 9 zip files available (01–09), each ~11 MB / ~2M rows (10–14 removed by Microsoft)
  - CSV format (no header): taxi_id, timestamp (no timezone — stored as UTC to match GeoMesa), longitude, latitude

Usage (run from geomesa-trino/):
  python tools/ingest_tdrive.py              # download 1 zip (~2M rows)
  python tools/ingest_tdrive.py --zips 9     # download all 9 available (~9M rows)
  python tools/ingest_tdrive.py --no-download  # use zips already in tools/data/tdrive/
"""
import argparse
import io
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import requests
import trino
from shapely.geometry import Point
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BinaryType, IntegerType,
    NestedField, StringType, TimestamptzType,
)

sys.path.insert(0, ".")
from common import (
    trino_connect,
    GeometryColumn, TABLE_PROPERTIES, ParseStats, chunked_append,
    companion_fields, enable_pyiceberg_nested_metrics,
    geom_to_wkb, local_rest_catalog, metrics_properties_for, optimize_tables,
    partition_spec_for_geoms, with_companion_columns,
)

enable_pyiceberg_nested_metrics()

# T-drive is geographically concentrated (Beijing, ~1 deg box), so a finer Z2 grid
# only adds partitions where data actually lands — query-side pruning gets tighter
# without inflating global file count the way it would for a worldwide dataset.
# At bits=20 (~0.35 deg cells), Beijing splits into ~45 cells of ~45k rows each;
# the target-file-size override consolidates each cell into one file so per-file
# overhead doesn't dominate.
Z2_BITS    = 20
TARGET_FILE_SIZE_BYTES = 4 * 1024 * 1024  # 4 MiB

DATA_DIR   = Path("tools/data/tdrive")
BASE_URL   = "https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02"

CATALOG = local_rest_catalog()

GEOMS        = [GeometryColumn(name="geom", index="z2", z2_bits=Z2_BITS)]


def _iceberg_schema(geoms: list[GeometryColumn]) -> Schema:
    base = [
        NestedField(1, "__fid__",   StringType(),   required=True),
        NestedField(2, "geom",      BinaryType()),
        NestedField(3, "dtg",       TimestamptzType()),
        NestedField(4, "taxi_id",   IntegerType()),
    ]
    return Schema(*base, *companion_fields(geoms, next_id=5))


def download_zip(n: int) -> Path:
    path = DATA_DIR / f"0{n}.zip"
    if path.exists():
        print(f"  Already have 0{n}.zip ({path.stat().st_size // 1024 // 1024} MB)")
        return path
    url = f"{BASE_URL}/0{n}.zip"
    print(f"  Downloading 0{n}.zip ...")
    resp = requests.get(url, stream=True, timeout=120,
                        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"})
    resp.raise_for_status()
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f:
        for chunk in resp.iter_content(65536):
            f.write(chunk)
    print(f"  Saved 0{n}.zip ({path.stat().st_size // 1024 // 1024} MB)")
    return path


def build_table(rows: list, *, geoms: list[GeometryColumn]) -> pa.Table:
    fids, wkbs, dtgs, taxi_ids = [], [], [], []
    for fid, wkb, dtg, taxi_id in rows:
        fids.append(fid)
        wkbs.append(wkb)
        dtgs.append(dtg)
        taxi_ids.append(taxi_id)
    base = pa.table({
        "__fid__":  pa.array(fids,     pa.string()),
        "geom":     pa.array(wkbs,     pa.large_binary()),
        "dtg":      pa.array(dtgs,     pa.timestamp("us", tz="UTC")),
        "taxi_id":  pa.array(taxi_ids, pa.int32()),
    }, schema=pa.schema([
        pa.field("__fid__", pa.string(), nullable=False),
        pa.field("geom",    pa.large_binary()),
        pa.field("dtg",     pa.timestamp("us", tz="UTC")),
        pa.field("taxi_id", pa.int32()),
    ]))
    return with_companion_columns(base, geoms)


def parse_zip_iter(zip_path: Path, zip_num: str, stats):
    """Stream-parse the zip, yielding row tuples one at a time."""
    with zipfile.ZipFile(zip_path) as zf:
        csv_names = [n for n in zf.namelist() if n.endswith(".txt")]
        for csv_name in csv_names:
            with zf.open(csv_name) as raw:
                for line in io.TextIOWrapper(raw, encoding="utf-8", errors="replace"):
                    line = line.strip()
                    if not line:
                        continue
                    parts = line.split(",")
                    if len(parts) != 4:
                        stats.skipped += 1
                        continue
                    try:
                        taxi_id = int(parts[0])
                        dtg = datetime.strptime(
                            parts[1].strip(), "%Y-%m-%d %H:%M:%S"
                        ).replace(tzinfo=timezone.utc)
                        lon = float(parts[2])
                        lat = float(parts[3])
                    except (ValueError, OverflowError):
                        stats.skipped += 1
                        continue
                    # T-Drive carries GPS noise with out-of-range coordinates
                    # (e.g. lon 222.5). Drop them so they don't poison __geom_bbox__
                    # stats and crash downstream Z2 clustering (Z2SFC requires
                    # lon∈[-180,180], lat∈[-90,90]). Same guard as ingest_geolife/ais.
                    if not (-180.0 <= lon <= 180.0 and -90.0 <= lat <= 90.0):
                        stats.skipped += 1
                        continue
                    pt  = Point(lon, lat)
                    fid = f"{zip_num}_{taxi_id}_{int(dtg.timestamp())}"
                    yield (fid, geom_to_wkb(pt), dtg, taxi_id)


def setup_table(table_name: str, geoms: list[GeometryColumn], dtg_grain: str | None):
    fq = f"spatial.{table_name}"
    try:
        CATALOG.create_namespace("spatial")
    except NamespaceAlreadyExistsError:
        pass
    try:
        CATALOG.drop_table(fq)
        print(f"Dropped existing {fq}")
    except NoSuchTableError:
        pass
    schema = _iceberg_schema(geoms)
    properties = {
        **TABLE_PROPERTIES,
        **metrics_properties_for(geoms),
        "write.target-file-size-bytes": str(TARGET_FILE_SIZE_BYTES),
    }
    # tdrive z2: drop dtg partition. 1 week of data over ~25 z2 cells already
    # collapses to a small partition count; adding day-grain would multiply by 7
    # and produce excessively fragmented per-cell files.
    tbl = CATALOG.create_table(
        fq,
        schema=schema,
        partition_spec=partition_spec_for_geoms(schema, geoms, dtg_grain=dtg_grain),
        properties=properties,
    )
    label = ",".join(f"{g.name}/{g.index or 'none'}" for g in geoms)
    print(f"Created {fq} (geoms: {label})")
    print(f"  Schema fields  : {[f.name for f in tbl.schema().fields]}")
    return tbl


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--zips", type=int, default=1, metavar="N",
                        help="Number of zip files to download/process (1–9, default 1)")
    parser.add_argument("--no-download", action="store_true",
                        help="Skip download; process all zips already in tools/data/tdrive/")
    args = parser.parse_args()

    conn = trino_connect()
    try:
        conn.cursor().execute("SELECT 1").fetchall()
    except Exception as exc:
        print(f"Cannot reach Trino at localhost:8080 — is the stack running?\n{exc}")
        sys.exit(1)

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if args.no_download:
        zip_paths = sorted(DATA_DIR.glob("0*.zip"))
        if not zip_paths:
            print(f"No zip files in {DATA_DIR}. Remove --no-download to fetch them.")
            sys.exit(1)
        print(f"Using {len(zip_paths)} existing zip file(s) in {DATA_DIR}/")
    else:
        n = max(1, min(9, args.zips))
        print(f"Downloading {n} of 9 zip files to {DATA_DIR}/")
        zip_paths = [download_zip(i) for i in range(1, n + 1)]

    z2_tbl = setup_table("tdrive", GEOMS, dtg_grain=None)
    targets = [(lambda rows: build_table(rows, geoms=GEOMS), z2_tbl)]

    # Stream parse → chunked append. The identity(__geom_z2__) partition spec
    # gives spatial locality at the file level, so global sort is unnecessary.
    stats = ParseStats()

    def all_rows():
        for zip_path in zip_paths:
            print(f"\nParsing {zip_path.name} ...")
            yield from parse_zip_iter(zip_path, zip_path.stem, stats)

    written = chunked_append(all_rows(), targets)

    print(f"\nCompacting tables (within-partition file fragments) ...")
    optimize_tables(conn, ["tdrive"])

    print(f"\n{'─'*60}")
    print(f"  spatial.tdrive : {written:,} rows ingested")
    print(f"  Skipped        : {stats.skipped:,} (malformed / invalid coords)")
    print(f"  Run: make bench")


if __name__ == "__main__":
    main()
