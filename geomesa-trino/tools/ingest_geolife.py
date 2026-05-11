"""
Download, setup, and ingest GeoLife GPS trajectory data into spatial.geolife.

GeoLife: ~25M GPS points from 182 users in Beijing and globally, April 2007 – Aug 2012.
  - Single zip (~298 MB): Data/{user_id}/Trajectory/*.plt
  - PLT format (skip 6 header lines): lat, lon, 0, altitude_ft, days, date, time
  - Timestamps treated as UTC (GeoMesa reference.conf does no timezone conversion)

Usage (run from geomesa-trino/):
  python tools/ingest_geolife.py              # download and ingest
  python tools/ingest_geolife.py --no-download  # use zip already in tools/data/geolife/
"""
import argparse
import io
import sys
import zipfile
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import requests
import trino
from shapely.geometry import Point
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BinaryType, DoubleType,
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

# GeoLife is Beijing-concentrated with a global travel tail and spans ~12.5 years
# of GPS traces. bits=18 (~0.7 deg cells) gives neighborhood-scale spatial pruning
# without exploding the active-cell count from the global tail; year-grain temporal
# partitioning consolidates the long time range into ~13 buckets so files stay large
# enough for per-file stats to mean something.
Z2_BITS    = 18
DTG_GRAIN  = "year"
TARGET_FILE_SIZE_BYTES = 4 * 1024 * 1024  # 4 MiB

DATA_DIR = Path("tools/data/geolife")
ZIP_NAME = "Geolife Trajectories 1.3.zip"
ZIP_URL  = (
    "https://download.microsoft.com/download/F/4/8/"
    "F4894AA5-FDBC-481E-9285-D5F8C4C4F039/Geolife%20Trajectories%201.3.zip"
)

CATALOG = local_rest_catalog()

GEOMS        = [GeometryColumn(name="geom", index="z2", z2_bits=Z2_BITS)]


def _iceberg_schema(geoms: list[GeometryColumn]) -> Schema:
    base = [
        NestedField(1, "__fid__",       StringType(),   required=True),
        NestedField(2, "geom",          BinaryType()),
        NestedField(3, "dtg",           TimestamptzType()),
        NestedField(4, "user_id",       StringType()),
        NestedField(5, "track_id",      StringType()),
        NestedField(6, "altitude_ft",   DoubleType()),
    ]
    return Schema(*base, *companion_fields(geoms, next_id=7))


def download_zip() -> Path:
    path = DATA_DIR / ZIP_NAME
    if path.exists():
        print(f"  Already have {ZIP_NAME} ({path.stat().st_size // 1024 // 1024} MB)")
        return path
    print(f"  Downloading {ZIP_NAME} (~298 MB) ...")
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    resp = requests.get(ZIP_URL, stream=True, timeout=300,
                        headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status()
    with open(path, "wb") as f:
        for chunk in resp.iter_content(65536):
            f.write(chunk)
    print(f"  Saved {ZIP_NAME} ({path.stat().st_size // 1024 // 1024} MB)")
    return path


def _group_plt_files(zf: zipfile.ZipFile) -> dict[str, list[str]]:
    """Return {user_id: [plt_name, ...]} grouping from the zip namelist."""
    groups: dict[str, list[str]] = defaultdict(list)
    for name in zf.namelist():
        if not name.lower().endswith(".plt"):
            continue
        parts = name.replace("\\", "/").split("/")
        try:
            data_idx = next(i for i, p in enumerate(parts) if p == "Data")
            user_id  = parts[data_idx + 1]
            groups[user_id].append(name)
        except (StopIteration, IndexError):
            pass
    return groups


def _parse_user_iter(zf: zipfile.ZipFile, user_id: str,
                     plt_names: list[str], stats):
    """Yield row tuples for one user's PLT files."""
    for plt_name in plt_names:
        track_id = Path(plt_name.replace("\\", "/").split("/")[-1]).stem
        with zf.open(plt_name) as raw:
            for line_num, line in enumerate(
                io.TextIOWrapper(raw, encoding="utf-8", errors="replace")
            ):
                if line_num < 6:
                    continue
                line = line.strip()
                if not line:
                    continue
                fields = line.split(",")
                if len(fields) < 7:
                    stats.skipped += 1
                    continue
                try:
                    lat         = float(fields[0])
                    lon         = float(fields[1])
                    altitude_ft = float(fields[3])
                    dtg         = datetime.strptime(
                        fields[5].strip() + fields[6].strip(), "%Y-%m-%d%H:%M:%S"
                    ).replace(tzinfo=timezone.utc)
                except (ValueError, OverflowError):
                    stats.skipped += 1
                    continue
                if not (-180.0 <= lon <= 180.0 and -90.0 <= lat <= 90.0):
                    stats.skipped += 1
                    continue
                pt = Point(lon, lat)
                fid = f"{user_id}_{track_id}_{int(dtg.timestamp())}"
                yield (fid, geom_to_wkb(pt), dtg, user_id, track_id, altitude_ft)


def _parse_geolife_iter(zf: zipfile.ZipFile, user_files: dict, stats):
    """Yield rows across all users in user_id order."""
    for user_id in sorted(user_files.keys()):
        yield from _parse_user_iter(zf, user_id, user_files[user_id], stats)


def build_table(rows: list, *, geoms: list[GeometryColumn]) -> pa.Table:
    fids, wkbs, dtgs, uids, tids, alts = [], [], [], [], [], []
    for fid, wkb, dtg, uid, tid, alt in rows:
        fids.append(fid);  wkbs.append(wkb)
        dtgs.append(dtg);  uids.append(uid);  tids.append(tid);  alts.append(alt)
    base = pa.table({
        "__fid__":     pa.array(fids,     pa.string()),
        "geom":        pa.array(wkbs,     pa.large_binary()),
        "dtg":         pa.array(dtgs,     pa.timestamp("us", tz="UTC")),
        "user_id":     pa.array(uids,     pa.string()),
        "track_id":    pa.array(tids,     pa.string()),
        "altitude_ft": pa.array(alts,     pa.float64()),
    }, schema=pa.schema([
        pa.field("__fid__",     pa.string(), nullable=False),
        pa.field("geom",        pa.large_binary()),
        pa.field("dtg",         pa.timestamp("us", tz="UTC")),
        pa.field("user_id",     pa.string()),
        pa.field("track_id",    pa.string()),
        pa.field("altitude_ft", pa.float64()),
    ]))
    return with_companion_columns(base, geoms)


def setup_table(table_name: str, geoms: list[GeometryColumn]):
    try:
        CATALOG.create_namespace("spatial")
    except NamespaceAlreadyExistsError:
        pass
    fq = f"spatial.{table_name}"
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
    tbl = CATALOG.create_table(
        fq,
        schema=schema,
        partition_spec=partition_spec_for_geoms(schema, geoms, dtg_grain=DTG_GRAIN),
        properties=properties,
    )
    label = ",".join(f"{g.name}/{g.index or 'none'}" for g in geoms)
    print(f"Created {fq} (geoms: {label})")
    print(f"  Schema fields  : {[f.name for f in tbl.schema().fields]}")
    return tbl


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--no-download", action="store_true",
                        help="Skip download; use zip already in tools/data/geolife/")
    args = parser.parse_args()

    conn = trino_connect()
    try:
        conn.cursor().execute("SELECT 1").fetchall()
    except Exception as exc:
        print(f"Cannot reach Trino at localhost:8080 — is the stack running?\n{exc}")
        sys.exit(1)

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if args.no_download:
        zip_path = DATA_DIR / ZIP_NAME
        if not zip_path.exists():
            print(f"No zip found at {zip_path}. Remove --no-download to fetch it.")
            sys.exit(1)
        print(f"Using existing {ZIP_NAME}")
    else:
        zip_path = download_zip()

    z2_tbl = setup_table("geolife", GEOMS)
    targets = [(lambda rows: build_table(rows, geoms=GEOMS), z2_tbl)]

    # Stream parse → chunked append. The identity(__geom_z2__) partition spec
    # gives spatial locality at the file level, so global sort is unnecessary.
    stats = ParseStats()
    with zipfile.ZipFile(zip_path) as zf:
        user_files = _group_plt_files(zf)
        total_plt = sum(len(v) for v in user_files.values())
        print(f"\nParsing {total_plt} PLT files across {len(user_files)} users ...")
        written = chunked_append(_parse_geolife_iter(zf, user_files, stats), targets)

    print(f"\nCompacting tables (within-partition file fragments) ...")
    optimize_tables(conn, ["geolife"])

    print(f"\n{'─'*60}")
    print(f"  spatial.geolife : {written:,} rows ingested")
    print(f"  Skipped         : {stats.skipped:,} (malformed / invalid coords)")
    print(f"  Run: make bench")


if __name__ == "__main__":
    main()
