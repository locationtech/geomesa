"""
Download, setup, and ingest Marine Cadastre AIS vessel-tracking data into spatial.ais.

AIS: Automatic Identification System GPS pings from vessels in US waters.
  - Source: NOAA MarineCadastre (coast.noaa.gov/htdata/CMSP/AISDataHandler/)
  - Post-2015 format: CSV with header row
  - Columns: MMSI, BaseDateTime, LAT, LON, SOG, COG, Heading, VesselName, ..., VesselType, ...
  - Timestamps are UTC

Default download: Zone 17 (Mid-Atlantic/East Coast), January 2017 (~5M rows).
Each zone-month file is ~10-50 MB compressed; rows written in sorted Z2 chunks.

Usage (run from geomesa-trino/):
  python tools/ingest_ais.py                          # Zone 17, Jan 2017
  python tools/ingest_ais.py --year 2017 --month 7 --zone 17
  python tools/ingest_ais.py --no-download            # use zip already in tools/data/ais/
"""
import argparse
import csv
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
    BinaryType, DoubleType, IntegerType,
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

DATA_DIR   = Path("tools/data/ais")
BASE_URL   = "https://coast.noaa.gov/htdata/CMSP/AISDataHandler"


# AIS data is coastline-concentrated along Zone 17 (US East Coast, roughly
# -84 to -78 W × 24 to 45 N — but vessel positions cluster at ports, not
# spread evenly). At bits=16 (~1.4 deg × 0.7 deg cells), port-area queries
# can prune to a handful of cells; at bits=12 (5.6 deg × 2.8 deg) every
# port falls into a small set of cells and tight queries can't prune well.
# 16 MiB target file size keeps each cell's ~640k rows in ~3 files instead
# of dozens.
Z2_BITS    = 16
TARGET_FILE_SIZE_BYTES = 16 * 1024 * 1024


GEOMS        = [GeometryColumn(name="geom", index="z2", z2_bits=Z2_BITS)]


def _ais_url(year: int, month: int, zone: int) -> str:
    return f"{BASE_URL}/{year}/AIS_{year}_{month:02d}_Zone{zone:02d}.zip"


def _ais_zip_name(year: int, month: int, zone: int) -> str:
    return f"AIS_{year}_{month:02d}_Zone{zone:02d}.zip"


CATALOG = local_rest_catalog()


def _iceberg_schema(geoms: list[GeometryColumn]) -> Schema:
    base = [
        NestedField(1, "__fid__",       StringType(),   required=True),
        NestedField(2, "geom",          BinaryType()),
        NestedField(3, "dtg",           TimestamptzType()),
        NestedField(4, "mmsi",          IntegerType()),
        NestedField(5, "vessel_name",   StringType()),
        NestedField(6, "vessel_type",   IntegerType()),
        NestedField(7, "sog",           DoubleType()),
    ]
    return Schema(*base, *companion_fields(geoms, next_id=8))


def download_zip(year: int, month: int, zone: int) -> Path:
    name = _ais_zip_name(year, month, zone)
    path = DATA_DIR / name
    if path.exists():
        print(f"  Already have {name} ({path.stat().st_size // 1024 // 1024} MB)")
        return path
    url = _ais_url(year, month, zone)
    print(f"  Downloading {name} from {url} ...")
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    resp = requests.get(url, stream=True, timeout=300,
                        headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status()
    with open(path, "wb") as f:
        for chunk in resp.iter_content(65536):
            f.write(chunk)
    print(f"  Saved {name} ({path.stat().st_size // 1024 // 1024} MB)")
    return path


def _safe_int(s: str) -> int | None:
    try:
        return int(float(s)) if s.strip() else None
    except ValueError:
        return None


def _safe_float(s: str) -> float | None:
    try:
        return float(s) if s.strip() else None
    except ValueError:
        return None


def build_table(rows: list, *, geoms: list[GeometryColumn]) -> pa.Table:
    fids, wkbs = [], []
    dtgs, mmsis, names, vtypes, sogs = [], [], [], [], []
    for fid, wkb, dtg, mmsi, name, vtype, sog in rows:
        fids.append(fid);    wkbs.append(wkb)
        dtgs.append(dtg);    mmsis.append(mmsi);  names.append(name)
        vtypes.append(vtype); sogs.append(sog)
    base = pa.table({
        "__fid__":     pa.array(fids,     pa.string()),
        "geom":        pa.array(wkbs,     pa.large_binary()),
        "dtg":         pa.array(dtgs,     pa.timestamp("us", tz="UTC")),
        "mmsi":        pa.array(mmsis,    pa.int32()),
        "vessel_name": pa.array(names,    pa.string()),
        "vessel_type": pa.array(vtypes,   pa.int32()),
        "sog":         pa.array(sogs,     pa.float64()),
    }, schema=pa.schema([
        pa.field("__fid__",     pa.string(), nullable=False),
        pa.field("geom",        pa.large_binary()),
        pa.field("dtg",         pa.timestamp("us", tz="UTC")),
        pa.field("mmsi",        pa.int32()),
        pa.field("vessel_name", pa.string()),
        pa.field("vessel_type", pa.int32()),
        pa.field("sog",         pa.float64()),
    ]))
    return with_companion_columns(base, geoms)


def setup_table(table_name: str, geoms: list[GeometryColumn]):
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
    tbl = CATALOG.create_table(
        fq,
        schema=schema,
        # AIS demo dataset is 1 month — daily(dtg) gives ~93 partitions × ~10 chunks
        # ≈ 1000+ small files. Monthly grain collapses to ~3 partitions while still
        # preserving temporal pruning for multi-month ingests (each Feb/Mar/etc.
        # would land in its own partition).
        partition_spec=partition_spec_for_geoms(schema, geoms, dtg_grain="month"),
        properties=properties,
    )
    label = ",".join(f"{g.name}/{g.index or 'none'}" for g in geoms)
    print(f"Created {fq} (geoms: {label})")
    print(f"  Schema fields  : {[f.name for f in tbl.schema().fields]}")
    return tbl


def parse_zip_iter(zip_path: Path, stats):
    """Stream-parse the AIS CSV, yielding row tuples one at a time."""
    with zipfile.ZipFile(zip_path) as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_names:
            print("  No CSV file found in zip.")
            return
        csv_name = csv_names[0]
        print(f"  Parsing {csv_name} ...")

        emitted = 0
        with zf.open(csv_name) as raw:
            reader = csv.reader(io.TextIOWrapper(raw, encoding="utf-8", errors="replace"))
            next(reader, None)  # skip header row
            for fields in reader:
                if len(fields) < 11:
                    stats.skipped += 1
                    continue
                try:
                    mmsi   = _safe_int(fields[0])
                    dtg    = datetime.strptime(
                        fields[1].strip(), "%Y-%m-%dT%H:%M:%S"
                    ).replace(tzinfo=timezone.utc)
                    lat    = float(fields[2])
                    lon    = float(fields[3])
                    sog    = _safe_float(fields[4])
                    vtype  = _safe_int(fields[10]) if len(fields) > 10 else None
                    vname  = fields[7].strip() if len(fields) > 7 else ""
                except (ValueError, OverflowError):
                    stats.skipped += 1
                    continue
                # AIS "not available" codes: lat=91, lon=181
                if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
                    stats.skipped += 1
                    continue
                if mmsi is None:
                    stats.skipped += 1
                    continue
                pt = Point(lon, lat)
                fid = f"{mmsi}_{int(dtg.timestamp())}"
                yield (fid, geom_to_wkb(pt), dtg,
                       mmsi, vname, vtype if vtype is not None else -1,
                       sog if sog is not None else 0.0)
                emitted += 1
                if emitted % 500_000 == 0:
                    print(f"  Parsed {emitted:,} rows ...", end="\r")


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--year",  type=int, default=2017, help="Year (default 2017)")
    parser.add_argument("--month", type=int, default=1,    help="Month 1–12 (default 1)")
    parser.add_argument("--zone",  type=int, default=17,   help="AIS zone (default 17, Mid-Atlantic)")
    parser.add_argument("--no-download", action="store_true",
                        help="Skip download; use zip already in tools/data/ais/")
    args = parser.parse_args()

    conn = trino_connect()
    try:
        conn.cursor().execute("SELECT 1").fetchall()
    except Exception as exc:
        print(f"Cannot reach Trino at localhost:8080 — is the stack running?\n{exc}")
        sys.exit(1)

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if args.no_download:
        name = _ais_zip_name(args.year, args.month, args.zone)
        zip_path = DATA_DIR / name
        if not zip_path.exists():
            print(f"No zip found at {zip_path}. Remove --no-download to fetch it.")
            sys.exit(1)
        print(f"Using existing {name}")
    else:
        zip_path = download_zip(args.year, args.month, args.zone)

    z2_tbl = setup_table("ais", GEOMS)
    targets = [(lambda rows: build_table(rows, geoms=GEOMS), z2_tbl)]

    # Stream parse → chunked append. The identity(__geom_z2__) partition spec
    # gives spatial locality at the file level, so global sort is unnecessary.
    print(f"\nIngesting AIS data ({args.year}-{args.month:02d} Zone {args.zone:02d}) ...")
    stats = ParseStats()
    written = chunked_append(parse_zip_iter(zip_path, stats), targets)

    print(f"\nCompacting tables (within-partition file fragments) ...")
    optimize_tables(conn, ["ais"])

    print(f"\n{'─'*60}")
    print(f"  spatial.ais : {written:,} rows ingested")
    print(f"  Skipped     : {stats.skipped:,} (malformed / invalid coords)")
    print(f"  Run: make bench")


if __name__ == "__main__":
    main()
