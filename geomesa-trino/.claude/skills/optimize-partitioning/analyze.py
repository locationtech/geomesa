#!/usr/bin/env python3
"""
Analyze an Iceberg table's spatial+temporal layout and recommend a partition
spec sized for the dataset. Advisory only: prints findings + migration SQL,
never executes ALTER/CTAS.

What it does:
  1. Discover the geometry column, bbox/z2/xz2 companions, and time column
     from DESCRIBE.
  2. Profile row count, bbox extent, temporal range, per-file size from
     `<table>$files`.
  3. Score `(temporal_grain, truncate_N)` candidates by how close the *hot*
     partition's total bytes lands to --target-partition-mb. Skew-aware: a
     uniform mean hides one giant cell that kills pruning, so the analyzer
     ranks on max-bytes-per-partition, not mean.
  4. Emit a markdown report (default) or a restartable bash migration script
     (--script).

Common invocations:

  # Default report: profile the table and recommend a spec
  tools/.venv/bin/python .claude/skills/optimize-partitioning/analyze.py \\
    iceberg.trino_test.tdrive

  # Custom partition-size target + query envelope (tighter target → finer N)
  tools/.venv/bin/python .claude/skills/optimize-partitioning/analyze.py \\
    iceberg.trino_test.tdrive --target-partition-mb 64 --query-envelope-deg 0.5

  # Same scoring target but write 256 MB files inside each partition
  tools/.venv/bin/python .claude/skills/optimize-partitioning/analyze.py \\
    iceberg.trino_test.tdrive --target-partition-mb 1024 --target-file-mb 256

  # Step-by-step migration SQL instead of one inline block (easier paging)
  tools/.venv/bin/python .claude/skills/optimize-partitioning/analyze.py \\
    iceberg.trino_test.tdrive --steps

  # Precise distribution via GROUP BY scan instead of $files-metadata
  # estimate. Use when the report shows a "loosely z2-sorted" warning.
  tools/.venv/bin/python .claude/skills/optimize-partitioning/analyze.py \\
    iceberg.trino_test.tdrive --exact-skew

  # Restartable bash script that does the migration in data-balanced batches.
  # --out writes + chmods the file; without --out it goes to stdout and you
  # have to chmod it yourself.
  tools/.venv/bin/python .claude/skills/optimize-partitioning/analyze.py \\
    iceberg.trino_test.tdrive --script --out migrate_tdrive.sh

When to reach for which flag:
  --target-partition-mb  Bigger target → coarser N, fewer partitions, bigger
                         bytes-read per pruned partition. Bigger queries (or
                         I/O-bound stacks) usually want bigger partitions. The
                         scoring optimization target. (def: 128, was --target-
                         file-mb prior to the rename)
  --target-file-mb       Bigger target → fewer/larger Parquet files INSIDE each
                         partition. Affects the migration SQL's
                         iceberg.target_max_file_size only; doesn't affect the
                         recommendation itself. (def: 128)
  --query-envelope-deg Representative query side length; drives the reported
                       pruning rate. Match it to your typical workload. (def: 1.0)
  --exact-skew         Loosely-sorted source (e.g. Trino-CTAS-written) → default
                       histogram smears the hot cell. --exact-skew is precise
                       (slower; O(rows)).
  --steps              Splits the migration SQL into H3 sections with prose.
  --script             Emit a batched, restartable bash migration script
                       instead of the report. Recommended for any spec with
                       more than ~100 partitions (almost all of them).
  --out PATH           When using --script, write to PATH and chmod +x it
                       (otherwise the script goes to stdout).

Depends on the `trino` Python client (installed in tools/.venv by
`make install-trino`). If you see ModuleNotFoundError below, run that first.
"""
import argparse
import json
import math
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

try:
    import trino
except ModuleNotFoundError:
    sys.stderr.write(
        "The 'trino' Python client isn't on this interpreter's path. "
        "Run with the project venv:\n"
        "  tools/.venv/bin/python .claude/skills/optimize-partitioning/analyze.py ...\n"
        "Or set up the venv first: make install-trino\n"
    )
    sys.exit(2)


# ── Constants ─────────────────────────────────────────────────────────────────

GRAINS = [
    # (name, seconds_per_bucket, transform_function)
    ("year",  365 * 24 * 3600, "year"),
    ("month",  30 * 24 * 3600, "month"),
    ("day",         24 * 3600, "day"),
    ("hour",             3600, "hour"),
]

# Z2 truncate widths worth considering. N=1 reaches only 4 cells globally;
# N>6 produces unmanageable partition counts for most datasets.
N_CANDIDATES = [2, 3, 4, 5, 6]

# Write-side memory budget for the batched migration (--script). Each open
# partition buffers a Parquet row group up to PARQUET_WRITER_BLOCK_MB, so peak
# write memory ≈ open_partitions × block_size. The number of distinct partitions
# one INSERT batch may touch is therefore capped by WRITE_BUFFER_BUDGET_MB /
# block_size — NOT just Trino's per-writer hard cap, and invariant to writer
# count (more writers only spread the same buffers). Empirically, ~166-prefix
# batches at 16MB blocks (~2.7GB) loaded fine while a 560-prefix batch (~9GB)
# OOM'd the node. Raise WRITE_BUFFER_BUDGET_MB for a cluster with more heap.
PARQUET_WRITER_BLOCK_MB = 16     # also emitted as iceberg.parquet_writer_block_size in the script
WRITE_BUFFER_BUDGET_MB  = 4096   # ~4GB peak for open-partition buffers → 4096/16 = 256 partitions/batch


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class TableProfile:
    catalog: str
    schema: str
    table: str
    geom_col: str
    bbox_col: Optional[str]
    z2_col: Optional[str]         # the partition column name (e.g. "__geom_z2__")
    z2_kind: Optional[str]        # "z2" or "xz2" (or None if no spatial partition possible)
    z2_metric_key: Optional[str]  # readable_metrics key for z2 (case-preserved), for $files bounds
    time_col: Optional[str]
    total_rows: int
    bbox: tuple[float, float, float, float]  # (xmin, ymin, xmax, ymax)
    time_range: Optional[tuple[datetime, datetime]]
    bytes_per_row: float
    file_count: int
    avg_file_bytes: float
    current_spec: str


@dataclass
class Candidate:
    grain: Optional[str]            # "year", "month", "day", "hour", or None for no temporal
    truncate_n: Optional[int]       # 1..6 or None for no spatial
    time_buckets: int
    spatial_cells_active: int       # bbox-derived (what bbox extent COULD touch)
    spatial_cells_populated: int    # data-derived (cells the data ACTUALLY occupies)
    spatial_cells_reachable: int
    total_partitions: int
    rows_per_partition: float       # mean over populated partitions
    bytes_per_partition: float      # mean over populated partitions
    bytes_per_partition_max: float  # MAX over populated partitions — the hot partition
    bytes_per_partition_p99: float  # p99 — worst-case partition for high-frequency queries
    skew_ratio: float               # max/mean over populated partitions (1.0 = uniform, >>1 = skewed)
    distribution_source: str        # "data" (file metadata), "exact" (GROUP BY scan), or "bbox" (fallback estimate)
    max_file_span_n: int            # widest z2 prefix span any single file covers at this N
                                    # (1 = every file lives in one prefix, >>1 = loose sort, histogram approximate)
    sample_query_partitions: int    # how many partitions a representative tight query touches
    pruning_rate: float             # 1 - sample_query_partitions / total_partitions

    def fit_score(self, target_bytes: int) -> float:
        """Lower is better. Logarithmic deviation of the *max* partition from the target —
        not the mean. With skewed data the mean lies; the hot partition's file size is what
        determines whether your queries pay 10MB or 1GB to evaluate the bbox predicate."""
        if self.bytes_per_partition_max <= 0:
            return float("inf")
        return abs(math.log2(self.bytes_per_partition_max / target_bytes))


# ── Trino I/O ─────────────────────────────────────────────────────────────────

def connect(host: str, port: int):
    return trino.dbapi.connect(host=host, port=port, user="optimize-partitioning")


def q(conn, sql: str) -> list[tuple]:
    cur = conn.cursor()
    cur.execute(sql)
    return cur.fetchall()


def q_scalar(conn, sql: str):
    rows = q(conn, sql)
    return rows[0][0] if rows else None


# ── Discovery ─────────────────────────────────────────────────────────────────

def discover(conn, catalog: str, schema: str, table: str) -> TableProfile:
    """Inspect the table's columns and stats.

    Profile is built entirely from Iceberg metadata tables — DESCRIBE, SHOW CREATE,
    and `$files` aggregations — so cost is O(files), not O(rows). This matters for
    tables in the 10^8+ row range where a full SELECT min/max would scan the data.
    """
    fq       = f'"{catalog}"."{schema}"."{table}"'
    fq_files = f'"{catalog}"."{schema}"."{table}$files"'

    # Column list with types. Trino's DESCRIBE lowercases identifiers; the
    # underlying Iceberg schema may preserve case (e.g. "timeUp"). We map between
    # the two via readable_metrics key sampling below.
    rows = q(conn, f"DESCRIBE {fq}")
    col_types = {r[0]: r[1] for r in rows}
    col_names = set(col_types)

    # Geometry column: anything matching the X / __X_bbox__ / __X_{z2,xz2}__ pattern.
    geom_col = None
    for name in col_names:
        if name.startswith("__") and name.endswith("__"):
            continue
        has_bbox = f"__{name}_bbox__" in col_names
        has_z2   = f"__{name}_z2__"   in col_names
        has_xz2  = f"__{name}_xz2__"  in col_names
        if has_bbox or has_z2 or has_xz2:
            geom_col = name
            break
    if geom_col is None:
        die("No geometry column found. Skill requires X + __X_bbox__/__X_z2__/__X_xz2__ naming convention.")

    bbox_col = f"__{geom_col}_bbox__" if f"__{geom_col}_bbox__" in col_names else None
    has_xz2 = f"__{geom_col}_xz2__" in col_names
    has_z2  = f"__{geom_col}_z2__"  in col_names
    z2_col  = f"__{geom_col}_xz2__" if has_xz2 else (f"__{geom_col}_z2__" if has_z2 else None)
    z2_kind = "xz2" if has_xz2 else ("z2" if has_z2 else None)

    # Candidate time column from DESCRIBE (lowercase name).
    time_col_lc = None
    for name, t in col_types.items():
        if name.startswith("__") or name.startswith("ingest"):
            continue
        if "timestamp" in t.lower():
            time_col_lc = name
            break

    # Recover the original (case-preserving) field names from readable_metrics by
    # sampling one row from $files. Iceberg keeps original case in metrics keys
    # even though Trino's DESCRIBE lowercases column names.
    sample = q(conn, f"SELECT readable_metrics FROM {fq_files} LIMIT 1")
    metrics_keys = set()
    if sample and sample[0][0]:
        try:
            metrics_keys = set(json.loads(sample[0][0]).keys())
        except (json.JSONDecodeError, TypeError):
            pass

    def ci_lookup(name: Optional[str], suffix: str = "") -> Optional[str]:
        if name is None:
            return None
        target = (name + suffix).lower()
        for k in metrics_keys:
            if k.lower() == target:
                return k
        return None

    time_key  = ci_lookup(time_col_lc)
    z2_key    = ci_lookup(z2_col)
    xmin_key  = ci_lookup(bbox_col, ".xmin") if bbox_col else None
    ymin_key  = ci_lookup(bbox_col, ".ymin") if bbox_col else None
    xmax_key  = ci_lookup(bbox_col, ".xmax") if bbox_col else None
    ymax_key  = ci_lookup(bbox_col, ".ymax") if bbox_col else None

    # Aggregate everything from $files in one shot.
    selects = [
        "sum(record_count)",
        "sum(file_size_in_bytes)",
        "count(*)",
        f"min(cast(json_extract_scalar(readable_metrics, '$[\"{xmin_key}\"].lower_bound') as double))" if xmin_key else "cast(null as double)",
        f"min(cast(json_extract_scalar(readable_metrics, '$[\"{ymin_key}\"].lower_bound') as double))" if ymin_key else "cast(null as double)",
        f"max(cast(json_extract_scalar(readable_metrics, '$[\"{xmax_key}\"].upper_bound') as double))" if xmax_key else "cast(null as double)",
        f"max(cast(json_extract_scalar(readable_metrics, '$[\"{ymax_key}\"].upper_bound') as double))" if ymax_key else "cast(null as double)",
        f"min(json_extract_scalar(readable_metrics, '$[\"{time_key}\"].lower_bound'))" if time_key else "cast(null as varchar)",
        f"max(json_extract_scalar(readable_metrics, '$[\"{time_key}\"].upper_bound'))" if time_key else "cast(null as varchar)",
    ]
    row = q(conn, f"SELECT {', '.join(selects)} FROM {fq_files}")[0]
    (total_records_in_files, total_bytes, file_count,
     bxmin, bymin, bxmax, bymax, tmin_str, tmax_str) = row

    if not file_count:
        die(f"Table {fq} has no data files — nothing to profile.")

    file_count = int(file_count)
    total_bytes = int(total_bytes or 0)
    total_records_in_files = int(total_records_in_files or 0)
    bytes_per_row = total_bytes / max(total_records_in_files, 1)
    avg_file_bytes = total_bytes / max(file_count, 1)
    total_rows = total_records_in_files

    if bxmin is not None and bymin is not None and bxmax is not None and bymax is not None:
        bbox = (float(bxmin), float(bymin), float(bxmax), float(bymax))
    else:
        bbox = (-180.0, -90.0, 180.0, 90.0)

    time_range = None
    if tmin_str and tmax_str:
        # Iceberg readable_metrics emits ISO 8601 timestamps; parse defensively.
        time_range = (_parse_iso(tmin_str), _parse_iso(tmax_str))

    # Current spec from SHOW CREATE.
    create_rows = q(conn, f"SHOW CREATE TABLE {fq}")
    create_sql = create_rows[0][0] if create_rows else ""
    current_spec = "(none)"
    for line in create_sql.splitlines():
        s = line.strip()
        if s.startswith("partitioning"):
            current_spec = s.rstrip(",")
            break

    return TableProfile(
        catalog=catalog, schema=schema, table=table,
        geom_col=geom_col, bbox_col=bbox_col, z2_col=z2_col, z2_kind=z2_kind,
        z2_metric_key=z2_key,
        time_col=time_key or time_col_lc,
        total_rows=total_rows, bbox=bbox, time_range=time_range,
        bytes_per_row=bytes_per_row,
        file_count=file_count, avg_file_bytes=avg_file_bytes,
        current_spec=current_spec,
    )


def _parse_iso(s: str) -> datetime:
    """Parse Iceberg readable_metrics timestamp strings. Handles seconds-precision
    ('2024-09-04T15:06:00') and microsecond/Z variants ('2024-09-04T15:06:00.000Z')."""
    s = s.rstrip("Z")
    # datetime.fromisoformat handles both '2024-09-04T15:06:00' and microseconds.
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        # Last resort: strip fractional seconds.
        return datetime.fromisoformat(s.split(".")[0])


# ── Recommendation ────────────────────────────────────────────────────────────

def reachable_cells(n: int) -> int:
    """With the `Z2SFC.index(...) << 2` shift applied before hex-encoding (per
    `tools/common.py::geom_z2_raw_hex`), the hemisphere bits land in the top hex
    position and all 16 hex digits are reachable at every truncate position.
    Total reachable prefixes at width N is 16^N (= per-axis^2)."""
    return 16 ** n


def per_axis_cells(n: int) -> int:
    """4N bits total → 2N bits per axis → 2^(2N) cells per axis."""
    return 1 << (2 * n)


def active_cells(n: int, bbox: tuple[float, float, float, float]) -> int:
    """Cells the dataset actually touches at this resolution."""
    xmin, ymin, xmax, ymax = bbox
    per_axis = per_axis_cells(n)
    cw = 360.0 / per_axis
    ch = 180.0 / per_axis
    nx = max(1, math.ceil((xmax - xmin) / cw))
    ny = max(1, math.ceil((ymax - ymin) / ch))
    return min(nx * ny, reachable_cells(n))


def query_cells(n: int, envelope_deg: float) -> int:
    """How many spatial cells a query of `envelope_deg` square touches."""
    per_axis = per_axis_cells(n)
    cw = 360.0 / per_axis
    ch = 180.0 / per_axis
    return max(1, math.ceil(envelope_deg / cw)) * max(1, math.ceil(envelope_deg / ch))


def time_buckets(profile: TableProfile, grain: Optional[str]) -> int:
    """Distinct temporal buckets the dataset spans at `grain` resolution."""
    if grain is None or profile.time_range is None:
        return 1
    span_s = (profile.time_range[1] - profile.time_range[0]).total_seconds()
    secs_per_bucket = next(g[1] for g in GRAINS if g[0] == grain)
    return max(1, math.ceil(span_s / secs_per_bucket))


# ── On-disk cache for query results keyed by (table, snapshot_id) ────────────
#
# The two expensive Trino-side fetches — readable_metrics from $files (used by
# the metadata-spreading path) and per-N GROUP BY truncate(z2,N) (--exact-skew)
# — are both pure functions of the table's current snapshot. Caching their
# outputs to disk lets follow-up runs (e.g. tweaking --target-partition-mb)
# skip Trino entirely and finish in milliseconds.
#
# Cache key = snapshot_id, which Iceberg bumps on every commit. Any write to
# the table invalidates the cache automatically — no manual invalidation.
#
# Layout:  <cache-dir>/<catalog>__<schema>__<table>/<snapshot_id>.<kind>[.nN].json
#   kind  = "metadata"   → list of [z2_lo, z2_hi, record_count] triples
#   kind  = "exact-skew" → list of per-prefix row counts; one file per N

DEFAULT_CACHE_DIR = Path.home() / ".cache" / "optimize-partitioning"


def _table_cache_dir(profile: 'TableProfile', base: Path) -> Path:
    """One sub-dir per fully-qualified table. Sanitizes path separators in
    case a catalog/schema name contains them (uncommon but possible)."""
    safe = lambda s: s.replace("/", "_").replace(":", "_")
    return base / f"{safe(profile.catalog)}__{safe(profile.schema)}__{safe(profile.table)}"


def _cache_path(profile: 'TableProfile', kind: str, n: Optional[int] = None) -> Optional[Path]:
    """Return the cache file path for this (table, snapshot, kind[, n]) tuple,
    or None if caching is disabled (no cache_dir stashed on profile, or no
    snapshot_id discovered)."""
    base = getattr(profile, "_cache_dir", None)
    snap = getattr(profile, "_snapshot_id", None)
    if base is None or snap is None:
        return None
    suffix = f".n{n}" if n is not None else ""
    return _table_cache_dir(profile, base) / f"{snap}.{kind}{suffix}.json"


def _load_cache(path: Optional[Path]):
    """Return cached JSON-decoded value or None on any miss/error. A read
    error logs a warning but doesn't abort — caller falls through to refetch."""
    if path is None or not path.exists():
        return None
    try:
        with open(path) as f:
            data = json.load(f)
        sys.stderr.write(f"✓ Loaded cache: {path.name} ({path.stat().st_size // 1024} kB)\n")
        return data
    except (OSError, json.JSONDecodeError) as e:
        sys.stderr.write(f"⚠ Cache read failed ({path}): {e}; refetching\n")
        return None


def _save_cache(path: Optional[Path], data) -> None:
    """Persist data to disk. Silent no-op when caching is disabled. A write
    failure is logged but never raised — caching is an optimization, not a
    correctness requirement."""
    if path is None:
        return
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(data, f, separators=(",", ":"))
        sys.stderr.write(f"💾 Saved cache: {path.name} ({path.stat().st_size // 1024} kB)\n")
    except OSError as e:
        sys.stderr.write(f"⚠ Cache write failed ({path}): {e}\n")


def _fetch_snapshot_id(conn, catalog: str, schema: str, table: str) -> Optional[str]:
    """Identify the table's current snapshot so we can key the cache on it.
    Cheap query — single row from $snapshots. Returns None if the table has
    no snapshots (empty table) or the system table isn't readable; in those
    cases caching is silently skipped."""
    fq_snap = f'"{catalog}"."{schema}"."{table}$snapshots"'
    try:
        rows = q(conn, f"SELECT snapshot_id FROM {fq_snap} ORDER BY committed_at DESC LIMIT 1")
        return str(rows[0][0]) if rows else None
    except Exception:
        return None


def _read_file_z2_intervals(conn, profile: TableProfile) -> Optional[list[tuple[str, str, int]]]:
    """Read per-file (z2_lower_str, z2_upper_str, record_count) tuples from
    `$files` metadata. Returns None if the table has no z2 partition column or
    the metadata doesn't expose z2 column stats. Cached on the profile object."""
    if not profile.z2_col:
        return None
    cached = getattr(profile, "_z2_intervals_cache", None)
    if cached is not None:
        # Cache uses [] as the "no stats available" sentinel; None means "not yet probed".
        return cached if cached else None

    # Disk-cache check before issuing the Trino query — same fetch is keyed on
    # snapshot_id, so any table mutation invalidates automatically.
    cpath = _cache_path(profile, "metadata")
    disk = _load_cache(cpath)
    if disk is not None:
        intervals = [(row[0], row[1], int(row[2])) for row in disk]
        object.__setattr__(profile, "_z2_intervals_cache", intervals)
        return intervals if intervals else None

    fq_files = f'"{profile.catalog}"."{profile.schema}"."{profile.table}$files"'
    z2k = profile.z2_metric_key or profile.z2_col
    jlo = f"json_extract_scalar(readable_metrics, '$[\"{z2k}\"].lower_bound')"
    jhi = f"json_extract_scalar(readable_metrics, '$[\"{z2k}\"].upper_bound')"
    try:
        rows = q(conn,
            f"SELECT {jlo}, {jhi}, record_count FROM {fq_files} "
            f"WHERE record_count > 0 AND {jlo} IS NOT NULL")
    except Exception:
        # Some catalogs/versions don't expose readable_metrics; fall back gracefully.
        object.__setattr__(profile, "_z2_intervals_cache", [])
        return None
    intervals = [(r[0] or "", r[1] or (r[0] or ""), int(r[2] or 0)) for r in rows]
    object.__setattr__(profile, "_z2_intervals_cache", intervals)
    # Persist for follow-up runs. JSON-serializable form: list of [lo, hi, n].
    _save_cache(cpath, [[lo, hi, n] for (lo, hi, n) in intervals])
    return intervals if intervals else None


def _spatial_distribution_at_n(intervals: list[tuple[str, str, int]], n: int) -> tuple[list[float], int]:
    """Spread each file's record_count uniformly across the truncate-N prefixes
    its z2 span covers. Returns (per-prefix row estimates, max_file_span_n).

    `max_file_span_n` is the widest z2-prefix range any single file covers at
    this N. When this is 1, every file lives entirely inside one truncate-N
    prefix and the histogram is exact. When it's large, the source is
    loosely z2-sorted and the uniform-within-span spread smears each file's
    rows across many prefixes — flattening the apparent max and inflating
    the populated-cell count. Detection enables a loud "results approximate"
    warning in the report; ground truth requires `--exact-skew`."""
    hist: dict[int, float] = {}
    max_span = 1
    for lo, hi, rc in intervals:
        lo_int = _pref_int(lo, n)
        hi_int = _pref_int(hi or lo, n)
        if hi_int < lo_int:
            hi_int = lo_int
        span = hi_int - lo_int + 1
        if span > max_span:
            max_span = span
        per = rc / span
        for p in range(lo_int, hi_int + 1):
            hist[p] = hist.get(p, 0.0) + per
    return list(hist.values()), max_span


def _exact_distribution_at_n(conn, profile: TableProfile, n: int) -> Optional[list[float]]:
    """Run a single `SELECT substr(z2, 1, N), COUNT(*) GROUP BY 1` against the
    data to get the EXACT per-prefix row distribution. O(rows), not O(files) —
    seconds on 10^7-row tables, longer on multi-billion-row ones. Cached on
    the profile object so multiple candidates at the same N reuse one scan."""
    if not profile.z2_col:
        return None
    cache_key = f"_exact_hist_n{n}"
    cached = getattr(profile, cache_key, None)
    if cached is not None:
        return cached

    # Per-N disk cache. A 10-15 minute GROUP BY for one N becomes a sub-second
    # JSON load on follow-up runs against the same snapshot.
    cpath = _cache_path(profile, "exact-skew", n=n)
    disk = _load_cache(cpath)
    if disk is not None:
        out = [float(x) for x in disk]
        object.__setattr__(profile, cache_key, out)
        return out

    fq = f'"{profile.catalog}"."{profile.schema}"."{profile.table}"'
    rows = q(conn,
        f"SELECT COUNT(*) FROM {fq} GROUP BY substr({profile.z2_col}, 1, {n})")
    out = [float(r[0]) for r in rows]
    object.__setattr__(profile, cache_key, out)
    _save_cache(cpath, out)
    return out


def score_candidate(conn, profile: TableProfile, grain: Optional[str], n: Optional[int],
                     envelope_deg: float, exact_skew: bool = False) -> Candidate:
    t_buckets = time_buckets(profile, grain)
    s_active  = active_cells(n, profile.bbox) if n else 1
    s_reach   = reachable_cells(n) if n else 1

    # Distribution source: exact GROUP BY scan (--exact-skew) > file metadata > bbox.
    spatial_rows = None
    max_file_span_n = 1
    dist_source = "bbox"
    if n and exact_skew:
        spatial_rows = _exact_distribution_at_n(conn, profile, n)
        if spatial_rows:
            dist_source = "exact"
    if spatial_rows is None and n:
        intervals = _read_file_z2_intervals(conn, profile)
        if intervals:
            spatial_rows, max_file_span_n = _spatial_distribution_at_n(intervals, n)
            dist_source = "data"

    if spatial_rows:
        s_pop = len(spatial_rows) or 1
        spatial_rows_max = max(spatial_rows)
        spatial_rows_mean = sum(spatial_rows) / s_pop
        spatial_rows.sort(reverse=True)
        # p99 = value at 1% percentile from the top (rank ceil(0.01 * N))
        p99_idx = max(0, min(s_pop - 1, math.ceil(0.01 * s_pop) - 1))
        spatial_rows_p99 = spatial_rows[p99_idx]
        skew_ratio = spatial_rows_max / spatial_rows_mean if spatial_rows_mean > 0 else 1.0
    else:
        s_pop = s_active
        spatial_rows_mean = profile.total_rows / s_pop
        spatial_rows_max  = spatial_rows_mean
        spatial_rows_p99  = spatial_rows_mean
        skew_ratio = 1.0

    # Within a spatial cell, assume rows distribute roughly evenly across time
    # buckets. (Temporal skew exists but is usually mild compared to spatial
    # clustering; we don't have cheap metadata for it.)
    rows_per_mean = spatial_rows_mean / t_buckets
    rows_per_max  = spatial_rows_max  / t_buckets
    rows_per_p99  = spatial_rows_p99  / t_buckets
    bytes_per_mean = rows_per_mean * profile.bytes_per_row
    bytes_per_max  = rows_per_max  * profile.bytes_per_row
    bytes_per_p99  = rows_per_p99  * profile.bytes_per_row

    total_p = t_buckets * s_pop

    # Sample query: tight spatial envelope × one time bucket.
    q_spatial = query_cells(n, envelope_deg) if n else 1
    q_partitions = min(q_spatial, s_pop)
    pruning_rate = 1.0 - (q_partitions / total_p)

    return Candidate(
        grain=grain, truncate_n=n,
        time_buckets=t_buckets,
        spatial_cells_active=s_active,
        spatial_cells_populated=s_pop,
        spatial_cells_reachable=s_reach,
        total_partitions=total_p,
        rows_per_partition=rows_per_mean,
        bytes_per_partition=bytes_per_mean,
        bytes_per_partition_max=bytes_per_max,
        bytes_per_partition_p99=bytes_per_p99,
        skew_ratio=skew_ratio,
        distribution_source=dist_source,
        max_file_span_n=max_file_span_n,
        sample_query_partitions=q_partitions,
        pruning_rate=pruning_rate,
    )


def rank_candidates(conn, profile: TableProfile, target_partition_mb: int,
                     envelope_deg: float, exact_skew: bool = False) -> list[Candidate]:
    target_bytes = target_partition_mb * (1 << 20)
    grains = [g[0] for g in GRAINS] if profile.time_range else [None]
    ns     = N_CANDIDATES         if profile.z2_col       else [None]
    cands = [score_candidate(conn, profile, g, n, envelope_deg, exact_skew=exact_skew)
             for g in grains for n in ns]
    # Filter: drop candidates with absurd partition counts (> 100k).
    cands = [c for c in cands if c.total_partitions <= 100_000]
    cands.sort(key=lambda c: (c.fit_score(target_bytes), c.total_partitions))
    return cands


# ── Output ────────────────────────────────────────────────────────────────────

def fmt_bytes(n: float) -> str:
    if n >= 1 << 30: return f"{n / (1 << 30):.1f} GB"
    if n >= 1 << 20: return f"{n / (1 << 20):.1f} MB"
    if n >= 1 << 10: return f"{n / (1 << 10):.1f} kB"
    return f"{n:.0f} B"


def fmt_count(n: float) -> str:
    if n >= 1e9: return f"{n / 1e9:.1f}B"
    if n >= 1e6: return f"{n / 1e6:.1f}M"
    if n >= 1e3: return f"{n / 1e3:.1f}k"
    return f"{n:.0f}"


def render_partitioning_array(profile: TableProfile, grain: Optional[str],
                                n: Optional[int]) -> str:
    """Render the `partitioning = ARRAY[...]` clause for CTAS.

    Lowercase the time column name: Trino canonicalizes unquoted identifiers
    to lowercase, and during CTAS partition-spec validation it matches the
    spec's source-column reference against the lowercased view of the new
    table's columns. A camelCase source column (e.g. PyIceberg's `timeUp`)
    survives in Iceberg metadata but resolves to `timeup` in Trino's scope,
    so a partition spec referencing `"timeUp"` fails with "Cannot find source
    column: timeUp". Lowercasing the reference is the portable fix.
    """
    parts = []
    if grain and profile.time_col:
        parts.append(f"'{grain}(\"{profile.time_col.lower()}\")'")
    if n and profile.z2_col:
        parts.append(f"'truncate({profile.z2_col}, {n})'")
    return "ARRAY[" + ", ".join(parts) + "]"


def partition_count_for(profile: TableProfile, grain: Optional[str], n: Optional[int]) -> int:
    """Total partition count the recommended spec produces (time buckets ×
    active spatial cells). Drives the writer-count calculation below."""
    t = time_buckets(profile, grain)
    s = active_cells(n, profile.bbox) if n else 1
    return t * s


def _writer_count_for(profile: TableProfile, grain: Optional[str], n: Optional[int]) -> int:
    """Smallest power-of-2 `task_max_writer_count` such that each writer handles
    <= 100 partitions (Trino's hard, non-raisable iceberg.max_partitions_per_writer
    cap). Capped at 64 — beyond that, single-node memory pressure from concurrent
    open writers makes a single CTAS impractical and the load should be batched."""
    total = partition_count_for(profile, grain, n)
    needed = max(1, math.ceil(total / 100))
    pow2 = 1
    while pow2 < needed:
        pow2 *= 2
    return min(pow2, 64)


def representative_envelope(profile: TableProfile, side_deg: float) -> tuple[float, float, float, float]:
    """Build a `side_deg × side_deg` envelope centered on the dataset's bbox.
    Used to parameterize the Step 4 verify-prediction query so it actually
    intersects the data instead of being hardcoded to a single location.
    For a globally-distributed dataset the geometric center may not be where
    the data is densest (FAA flight data has bbox -180..180 but most rows
    over CONUS); the caveats note this limitation."""
    cx = (profile.bbox[0] + profile.bbox[2]) / 2.0
    cy = (profile.bbox[1] + profile.bbox[3]) / 2.0
    half = side_deg / 2.0
    # Clamp to WGS84 bounds so we don't generate a predicate outside the
    # globe (matters for tables whose bbox center is near a pole or the
    # antimeridian).
    return (
        max(-180.0, cx - half),
        max(-90.0,  cy - half),
        min( 180.0, cx + half),
        min(  90.0, cy + half),
    )


def _pref_int(z2: str, n: int) -> int:
    """Integer value of a z2 hex string's truncate-N (first-N-char) prefix, used
    to measure how many distinct truncate-N partitions a [lo, hi] span covers.
    Tolerates Iceberg's incremented upper-bound byte: its string-max truncation
    bumps the last retained code point, which can turn an 'f' into a non-hex 'g';
    such a position (and everything after) rounds up to the max hex digit."""
    out = []
    for ch in (z2 or "")[:n]:
        c = ch.lower()
        if c in "0123456789abcdef":
            out.append(c)
        else:
            out.extend("f" * (n - len(out)))   # non-hex (e.g. 'g'): round up
            break
    s = "".join(out).ljust(n, "0")
    return int(s, 16) if s else 0


def _min_writer_count(max_open_partitions: int) -> int:
    """Fewest power-of-2 `task_max_writer_count` keeping each writer under ~70
    open partitions (Trino's hard 100/writer cap × 0.7 skew margin).

    This is a FLOOR, not a target. Extra writers do NOT reduce write-buffer
    memory (that's open_partitions × block_size, fixed) — they add parallel
    scan/exchange pipelines, i.e. more heap. On a memory-bound node, more writers
    makes an OOM *worse* (a 1.2B-row/23-partition batch at 16 writers GC-death-
    spiralled a 24GB heap; the same shape at 8 committed), so the migration uses
    the smallest count that clears the hard cap and never inflates it."""
    needed = max(1, math.ceil(max_open_partitions / 70))
    pow2 = 1
    while pow2 < needed:
        pow2 *= 2
    return min(pow2, 64)


def _balanced_z2_ranges(conn, profile: TableProfile, grain: Optional[str],
                        n: int) -> tuple[list, dict]:
    """Derive data-balanced z2 ranges from Iceberg `$files` metadata — O(files),
    no data scan (seconds, versus a ~20-min full GROUP BY on a 5B-row table).

    Each data file carries `record_count` plus per-column lower/upper z2 bounds
    in `readable_metrics`. Sorting files by their z2 lower bound puts the rows in
    z2 order, which is enough to bin-pack contiguous batches that are (a) even in
    rows and (b) bounded in distinct partitions.

    The partition bound is the key trick: file metadata can't tell a populated
    prefix from an empty one (and files overlap heavily when the source isn't
    tightly clustered), so rather than *estimate* per-batch partitions we cap the
    count of *covered* truncate-N prefixes per batch. We spread each file's rows
    across the prefixes its z2 span covers to get an approximate per-prefix row
    map, then walk prefixes in order and cut every `max_cells = mem_cap/buckets`
    covered prefixes (or sooner, on the row target), where `mem_cap` is the
    write-buffer memory budget (WRITE_BUFFER_BUDGET_MB / block_size); the writer
    count is then derived as the minimum that clears Trino's per-writer cap (see
    _min_writer_count). Since the truly populated
    prefixes are a subset of the covered ones, a batch holds at most
    `covered × buckets` partitions — a hard ceiling, no estimation, and (unlike a
    raw range width) it doesn't overshoot across sparse gaps.

    Boundaries are truncate-N prefixes, so a partition never straddles two
    batches and the half-open ranges tile the keyspace exactly once. Files whose
    z2 span exceeds a batch's prefix range get scanned by more than one batch
    (weak pruning); `stats['weak_pruning']` flags that.

    Returns (batches, stats); batch = (lo_prefix, hi_prefix, est_rows, cells),
    lo/hi None at the open ends.
    """
    # Reuse the cached metadata fetch — this used to be a separate query that
    # duplicated the readable_metrics scan AND bypassed the on-disk cache. Now
    # both the scoring path (rank_candidates) and the script path share one
    # cacheable fetch.
    intervals = _read_file_z2_intervals(conn, profile)
    if not intervals:
        fq_files = f'"{profile.catalog}"."{profile.schema}"."{profile.table}$files"'
        raise RuntimeError(
            f"{fq_files} exposes no per-file z2 bounds in readable_metrics — the "
            f"table may not collect column stats on {profile.z2_col}, so file-"
            f"metadata balancing can't work here.")

    # Sort by z2 lower bound; on fixed-width lowercase hex strings,
    # alphabetical order matches numerical order.
    files = []  # (lo_int, hi_int, rows) sorted by z2 lower bound
    for lo_str, hi_str, rc in sorted(intervals, key=lambda r: r[0]):
        lo_int = _pref_int(lo_str, n)
        hi_int = _pref_int(hi_str or lo_str, n)
        files.append((lo_int, max(lo_int, hi_int), rc))

    total_rows    = sum(f[2] for f in files)
    max_file_span = max(f[1] - f[0] + 1 for f in files)

    # Estimated rows per truncate-N prefix: spread each file's record_count evenly
    # across the prefixes its z2 span covers. Uniform-within-file is only an
    # approximation used to BALANCE rows — never to attribute them; the run-time
    # z2 predicate attributes every row exactly. A prefix any file covers is
    # "covered"; the true populated-partition set is a subset of covered, so
    # capping covered-prefixes-per-batch is a hard upper bound on partitions
    # (unlike a raw range width, which overshoots across sparse gaps).
    hist: dict = {}
    for lo_int, hi_int, rc in files:
        per = rc / (hi_int - lo_int + 1)
        for p in range(lo_int, hi_int + 1):
            hist[p] = hist.get(p, 0.0) + per
    prefixes  = sorted(hist)
    n_covered = len(prefixes)

    buckets   = time_buckets(profile, grain) if grain else 1
    # Batch size (max open partitions) is set by the MEMORY budget; the writer
    # count is then DERIVED as the minimum that clears Trino's hard 100-open-
    # partitions-per-writer cap. Two heap regimes are in play and they pull
    # writer_count in OPPOSITE directions:
    #   • write-buffer heap ≈ open_partitions × block_size — bounded by mem_cap,
    #     and invariant to writer_count;
    #   • scan/exchange heap GROWS with writer_count — each writer is another
    #     parallel read+shuffle pipeline. A 1.2B-row / 23-partition batch at 16
    #     writers pinned a 24GB heap into a GC death-spiral; the same shape at 8
    #     committed fine. So writer_count is a floor (clear the cap), never raised
    #     to "go faster" — that is how you OOM a constrained node.
    mem_cap      = max(1, WRITE_BUFFER_BUDGET_MB // PARQUET_WRITER_BLOCK_MB)
    max_cells    = max(1, mem_cap // max(1, buckets))
    writer_count = _min_writer_count(max_cells * max(1, buckets))
    # Even-rows target: enough batches to satisfy the cell cap, then cut a batch
    # early once its rows reach 1.5x the resulting average.
    min_batches = max(1, math.ceil(n_covered / max_cells))
    row_cap     = max(1, int(1.5 * total_rows / min_batches))

    def hx(p: int) -> str:   # prefix int -> n-char lowercase hex
        return format(p, f"0{n}x")

    batches = []
    b_start = 0
    cur_rows = 0.0
    cur_cells = 0
    for idx, p in enumerate(prefixes):
        if idx > b_start and (cur_cells >= max_cells or cur_rows >= row_cap):
            batches.append((hx(prefixes[b_start]), hx(p), int(cur_rows), cur_cells))
            b_start, cur_rows, cur_cells = idx, 0.0, 0
        cur_rows  += hist[p]
        cur_cells += 1
    batches.append((hx(prefixes[b_start]), None, int(cur_rows), cur_cells))
    # Open the first batch's lower bound so nothing below the first covered prefix
    # can be missed (defensive; z2 is non-null in practice).
    batches[0] = (None, batches[0][1], batches[0][2], batches[0][3])

    stats = {
        "total_rows":      total_rows,
        "n_files":         len(files),
        "n_covered":       n_covered,
        "buckets":         buckets,
        "est_parts_max":   n_covered * buckets,
        "n_batches":       len(batches),
        "mem_cap":         mem_cap,
        "writer_count":    writer_count,
        "max_cells":       max_cells,
        "max_file_span":   max_file_span,
        "max_batch_cells": max(b[3] for b in batches),
        "max_batch_rows":  max(b[2] for b in batches),
        "min_batch_rows":  min(b[2] for b in batches),
        "weak_pruning":    max_file_span > max_cells,
    }
    return batches, stats


def render_migration_script(conn, profile: TableProfile, grain: Optional[str], n: int,
                              target_file_mb: int = 128) -> str:
    """Generate a restartable bash script that migrates the table to the
    recommended spec via DATA-BALANCED batched INSERTs.

    Splitting the load by a fixed leading hex char of the z2 column assumes the
    z2 keyspace is evenly populated; real spatial data is clustered, so a few hex
    buckets carry most of the rows and partitions and OOM the writer. This
    instead reads the distribution from Iceberg `$files` metadata (see
    `_balanced_z2_ranges`) and cuts variable-width z2 ranges that each hold a
    bounded, comparable amount of work.

    - Each batch covers a contiguous z2 range chosen so its (estimated) distinct-
      partition count stays under the open-writer budget (writers × ~100, with
      margin) and its row count stays near the per-batch average. The predicate
      is a z2 RANGE (`>= 'lo' AND < 'hi'`), so Iceberg per-file z2 stats prune
      non-matching source files — total source reads stay ≈ one full scan.
    - Restartable: a batch is skipped if the destination already has rows in its
      z2 range. Iceberg INSERTs commit atomically (a killed INSERT leaves zero
      rows), so "range already populated" reliably means "batch done." Re-running
      resumes from the first incomplete batch.
    """
    cat, sch  = profile.catalog, profile.schema
    src, dst  = profile.table, f"{profile.table}_v2"
    z2_col    = profile.z2_col or "__geom_z2__"
    spec_str  = render_partitioning_array(profile, grain, n)
    # Partition array with inner double-quotes escaped for the bash double-quoted
    # CREATE string.
    arr       = spec_str.replace('"', '\\"')

    batches, st = _balanced_z2_ranges(conn, profile, grain, n)
    # One "lo|hi" entry per batch (empty side = open bound). The trailing
    # `# comment` sits outside the quotes, so bash treats it as a comment, not
    # part of the value.
    ranges_block = "\n".join(
        f'  "{lo or ""}|{hi or ""}"   # rows≈{rows:,} partitions≤{cells}'
        for lo, hi, rows, cells in batches)

    # If a typical file's z2 span is wider than a batch's prefix range, the
    # z2-range predicates prune loosely and batches re-scan overlapping files.
    # That's still correct, just more I/O — surface it so the cost isn't a
    # surprise.
    cluster_note = ""
    if st["weak_pruning"]:
        cluster_note = (
            f"# ⚠ NOTE: source files span up to {st['max_file_span']:,} truncate({n}) "
            f"prefixes — wider than a batch's\n"
            f"#   prefix range ({st['max_cells']:,}). The source isn't tightly "
            f"z2-clustered, so the z2-range\n"
            f"#   predicates below prune source files only loosely: batches re-scan "
            f"overlapping files, so total\n"
            f"#   source I/O is several x a single scan (still correct, just slower). "
            f"Compacting/sorting the\n"
            f"#   source by z2 first would make each batch read far less.\n#\n")

    return f"""#!/usr/bin/env bash
#
# Restartable partition migration for {cat}.{sch}.{src}
#   target spec: {spec_str}
# Generated by the optimize-partitioning skill from Iceberg $files metadata
# (O(files), no data scan): {st['total_rows']:,} rows across {st['n_files']:,} data
# files covering {st['n_covered']:,} truncate({n}) prefixes (<= {st['est_parts_max']:,}
# partitions over {st['buckets']} time bucket(s)).
#
# Strategy: create the destination empty, then INSERT in {st['n_batches']} DATA-
# BALANCED batches. Spatial data is skewed, so a uniform hex-prefix split would
# overload dense regions (that OOMs the writer). Each batch here instead covers a
# contiguous z2 RANGE holding at most {st['max_cells']:,} covered prefixes
# (<= the memory cap {st['mem_cap']} = {WRITE_BUFFER_BUDGET_MB}MB write-buffer budget /
# {PARQUET_WRITER_BLOCK_MB}MB block; task_max_writer_count={st['writer_count']} is the fewest writers that
# clears Trino's 100-open-partitions-per-writer hard cap) with rows kept near the
# per-batch average ({st['min_batch_rows']:,}..{st['max_batch_rows']:,} per batch, vs
# ~{st['total_rows'] // st['n_batches']:,} avg). Bounding covered prefixes caps each
# batch's distinct-partition count to a hard ceiling without scanning the data.
# (Row counts are file-metadata estimates; a file straddling a boundary blurs the
# estimate, never correctness.)
#
{cluster_note}# The z2 RANGE predicate lets Iceberg's per-file z2 column-stat pruning skip
# non-matching source files. Re-run safe: a batch whose range already has rows in
# the destination is skipped (Iceberg INSERTs commit atomically).
#
# Override the Trino invocation if you're not using the local docker container:
#   TRINO='trino --server https://my-coordinator:8443' ./migrate.sh
#
set -euo pipefail

TRINO=${{TRINO:-"docker exec trino trino"}}
CATALOG={cat}
SCHEMA={sch}
SRC={src}
DST={dst}
Z2COL='{z2_col}'

# Session properties applied to every statement.
#  * task_max_writer_count is the FEWEST writers that clear the 100-open-
#    partitions-per-writer hard cap for this batch size — deliberately NOT higher.
#    Each extra writer is another parallel scan/exchange pipeline (more heap), and
#    that scan/exchange memory (not the write buffers) is what GC-death-spirals a
#    constrained node. Raising this to "go faster" is how you OOM — leave it.
#  * parquet_writer_block_size bounds each open writer's row-group buffer; peak
#    write memory ~= open_partitions x block_size.
#  * query_max_memory_per_node — CLUSTER-SPECIFIC; set to your node's query
#    memory pool (= JVM Xmx − memory.heap-headroom-per-node). 16GB suits a 24GB-
#    heap node. A value ABOVE the cluster's query.max-memory-per-node config is
#    silently clamped, so check `SHOW SESSION` / config if unsure. NB this bounds
#    only TRACKED user memory; untracked encode/write buffers ride on top toward
#    Xmx, so it can't fully prevent a heap death-spiral — task_max_writer_count
#    above is the real control. Lower it for a smaller heap.
SESSION=(
  --session task_max_writer_count={st['writer_count']}
  --session iceberg.target_max_file_size={target_file_mb}MB
  --session iceberg.parquet_writer_block_size={PARQUET_WRITER_BLOCK_MB}MB
  --session query_max_memory=16GB
  --session query_max_memory_per_node=16GB
)

run()    {{ $TRINO --catalog "$CATALOG" --schema "$SCHEMA" "${{SESSION[@]}}" --output-format TSV --execute "$1"; }}
scalar() {{ run "$1" | tr -d '[:space:]'; }}

echo "== 1. create destination (idempotent) =="
# object_store_layout_enabled=true => Iceberg write.object-storage.enabled: hashes
# data-file paths across S3 prefixes so this large table's I/O spreads instead of
# hot-partitioning (native Trino property; raw write.* keys are blocked here).
run "CREATE TABLE IF NOT EXISTS \\"$CATALOG\\".\\"$SCHEMA\\".\\"$DST\\" \\
     WITH (partitioning = {arr}, format='PARQUET', format_version=2, object_store_layout_enabled=true) \\
     AS SELECT * FROM \\"$CATALOG\\".\\"$SCHEMA\\".\\"$SRC\\" WHERE false"

# Data-balanced z2 ranges, computed from Iceberg $files metadata at generation
# time. Format "lo|hi"; an empty side is an open bound. Contiguous and
# non-overlapping — each batch's hi is the next batch's lo — so they tile the
# whole z2 keyspace exactly once.
RANGES=(
{ranges_block}
)

echo "== 2. ${{#RANGES[@]}} data-balanced batched inserts on $Z2COL =="
for idx in "${{!RANGES[@]}}"; do
  lo="${{RANGES[$idx]%%|*}}"
  hi="${{RANGES[$idx]##*|}}"

  if [[ -n "$lo" && -n "$hi" ]]; then
    pred="\\"$Z2COL\\" >= '$lo' AND \\"$Z2COL\\" < '$hi'"
  elif [[ -n "$hi" ]]; then
    pred="\\"$Z2COL\\" < '$hi'"            # first batch: no lower bound
  elif [[ -n "$lo" ]]; then
    pred="\\"$Z2COL\\" >= '$lo'"           # last batch: no upper bound
  else
    pred="true"                           # single batch: whole table
  fi
  label="[${{lo:-^}},${{hi:-\\$}})"

  existing=$(scalar "SELECT count(*) FROM \\"$CATALOG\\".\\"$SCHEMA\\".\\"$DST\\" WHERE $pred")
  if [[ "$existing" =~ ^[0-9]+$ ]] && (( existing > 0 )); then
    echo "  [skip] batch $idx $label — destination already has $existing rows"
    continue
  fi

  echo "  [run ] batch $idx $label — INSERT ..."
  run "INSERT INTO \\"$CATALOG\\".\\"$SCHEMA\\".\\"$DST\\" \\
       SELECT * FROM \\"$CATALOG\\".\\"$SCHEMA\\".\\"$SRC\\" WHERE $pred"
  echo "  [done] batch $idx $label"
done

echo "== 3. verify row count parity =="
old=$(scalar "SELECT count(*) FROM \\"$CATALOG\\".\\"$SCHEMA\\".\\"$SRC\\"")
new=$(scalar "SELECT count(*) FROM \\"$CATALOG\\".\\"$SCHEMA\\".\\"$DST\\"")
echo "  source=$old  destination=$new"
if [[ "$old" == "$new" ]]; then
  echo "  ✓ row counts match"
  echo
  echo "Next, once you've validated queries against $DST:"
  echo "  ALTER TABLE \\"$CATALOG\\".\\"$SCHEMA\\".\\"$SRC\\" RENAME TO {src}_pre_repartition;"
  echo "  ALTER TABLE \\"$CATALOG\\".\\"$SCHEMA\\".\\"$DST\\" RENAME TO {src};"
else
  echo "  ✗ MISMATCH — do not swap names; investigate before proceeding."
  exit 1
fi
"""


def render_ctas(profile: TableProfile, grain: Optional[str], n: Optional[int],
                  envelope_deg: float = 1.0, step_by_step: bool = False,
                  target_file_mb: int = 128) -> str:
    """Render the CTAS migration script. Two layouts:
    - default: one fenced code block with numbered comments inside (compact, reproducible).
    - step_by_step: each step as a separate H3 section with prose + its own fenced block
      (easier to copy-paste step-by-step, survives CLI pagers that truncate long blocks).
    """
    if step_by_step:
        return _render_ctas_steps(profile, grain, n, envelope_deg, target_file_mb=target_file_mb)
    fq_old      = f'"{profile.catalog}"."{profile.schema}"."{profile.table}"'
    fq_new      = f'"{profile.catalog}"."{profile.schema}"."{profile.table}_v2"'
    fq_bak      = f'"{profile.catalog}"."{profile.schema}"."{profile.table}_pre_repartition"'
    fq_new_meta = f'"{profile.catalog}"."{profile.schema}"."{profile.table}_v2$files"'
    bbox_col    = profile.bbox_col or "__geom_bbox__"
    z2_col      = profile.z2_col or "__geom_z2__"
    arr         = render_partitioning_array(profile, grain, n)
    env_minx, env_miny, env_maxx, env_maxy = representative_envelope(profile, envelope_deg)
    return (
        "-- 0. Session properties for the CTAS write.\n"
        "--    THE WRITE-SIDE GOTCHA: iceberg.max_partitions_per_writer is hard-\n"
        "--    capped at 100 and CANNOT be raised. The write hash-distributes\n"
        "--    partition keys across task_max_writer_count writers, so each\n"
        "--    writer handles (total_partitions / task_max_writer_count)\n"
        "--    partitions — and that quotient must stay <= 100 or the CTAS fails\n"
        "--    with \"Exceeded limit of 100 open writers for partitions\".\n"
        "--    Pick task_max_writer_count (a power of 2) >= total_partitions/100.\n"
        "--    See the 'Write-side partition limit' note in the report for the\n"
        "--    value computed for THIS spec. Sorting the input doesn't help —\n"
        "--    Trino drops ORDER BY / sorted_by from a CTAS plan.\n"
        "SET SESSION query_max_memory = '20GB';\n"
        "SET SESSION query_max_memory_per_node = '20GB';\n"
        "SET SESSION query_max_execution_time = '6h'; -- overrides any 60s cap from a prior monitor session\n"
        f"SET SESSION iceberg.target_max_file_size = '{target_file_mb}MB';\n"
        f"SET SESSION task_max_writer_count = {_writer_count_for(profile, grain, n)};\n"
        "SET SESSION iceberg.parquet_writer_block_size = '16MB'; -- caps per-open-writer buffer\n"
        "\n"
        f"-- 1. Create the new table with the recommended spec. Compression +\n"
        f"--    file-size targeting come from session properties (set in step 0)\n"
        f"--    and catalog config — Trino blocks write.* keys in extra_properties.\n"
        f"--    object_store_layout_enabled=true is the native Trino property for\n"
        f"--    Iceberg write.object-storage.enabled: hashes data-file paths across\n"
        f"--    S3 prefixes so large-table I/O spreads instead of hot-partitioning.\n"
        f"CREATE TABLE {fq_new}\n"
        f"WITH (\n"
        f"    partitioning = {arr},\n"
        f"    format = 'PARQUET',\n"
        f"    format_version = 2,\n"
        f"    object_store_layout_enabled = true\n"
        f") AS\n"
        f"SELECT * FROM {fq_old};\n"
        f"\n"
        f"-- 2. Verify row count parity before swapping.\n"
        f"SELECT (SELECT count(*) FROM {fq_old}) AS old_count,\n"
        f"       (SELECT count(*) FROM {fq_new}) AS new_count;\n"
        f"\n"
        f"-- 3. Confirm partition values are the expected truncate-string prefixes\n"
        f"--    (e.g. 3-char lowercase hex for truncate(N=3)). If the rewrite ran\n"
        f"--    through a properly-configured writer this will look right; if you\n"
        f"--    see decimal-formatted integers (\"02\", \"08\", ...) the writer is\n"
        f"--    bypassing Iceberg's standard Truncate transform and the rewrite\n"
        f"--    didn't actually help — fix the writer before retrying.\n"
        f"SELECT DISTINCT partition.{z2_col}_trunc AS partition_value\n"
        f"FROM {fq_new_meta}\n"
        f"ORDER BY 1 LIMIT 20;\n"
        f"\n"
        f"-- 4. Confirm the spatial pruning improvement against a representative\n"
        f"--    tight query (envelope centered on the dataset's bbox). Same\n"
        f"--    predicate the Iceberg pruner uses on each file's per-leaf bbox\n"
        f"--    stats — runs in seconds on $files metadata, no data scan.\n"
        f"SELECT count(*) AS files_surviving\n"
        f"FROM {fq_new_meta}\n"
        f"WHERE cast(json_extract_scalar(readable_metrics, '$[\"{bbox_col}.xmax\"].upper_bound') AS double) >= {env_minx}\n"
        f"  AND cast(json_extract_scalar(readable_metrics, '$[\"{bbox_col}.xmin\"].lower_bound') AS double) <= {env_maxx}\n"
        f"  AND cast(json_extract_scalar(readable_metrics, '$[\"{bbox_col}.ymax\"].upper_bound') AS double) >= {env_miny}\n"
        f"  AND cast(json_extract_scalar(readable_metrics, '$[\"{bbox_col}.ymin\"].lower_bound') AS double) <= {env_maxy};\n"
        f"-- Repeat on the OLD table to compare. Expect this number to be much\n"
        f"-- smaller after the rewrite — that's the spatial-pruning win.\n"
        f"-- (Adjust the envelope to a location dense in your data if the bbox\n"
        f"-- center is empty — e.g. globally-distributed bboxes center on (0,0)\n"
        f"-- which is rarely where real data sits.)\n"
        f"\n"
        f"-- 5. Swap names (separate ALTERs — Trino doesn't support multi-rename).\n"
        f"ALTER TABLE {fq_old} RENAME TO {fq_bak};\n"
        f"ALTER TABLE {fq_new} RENAME TO {fq_old};\n"
        f"\n"
        f"-- 6. Once you've verified queries work against the renamed table:\n"
        f"-- DROP TABLE {fq_bak};\n"
    )


def _render_ctas_steps(profile: TableProfile, grain: Optional[str], n: Optional[int],
                         envelope_deg: float = 1.0, target_file_mb: int = 128) -> str:
    """One H3 section per step, with prose explanation between code blocks.
    Format optimized for copy-paste into a `trino>` session where the user
    runs steps sequentially and reviews output between them. Resilient to
    CLI pagers that truncate long markdown — each step stands alone."""
    fq_old      = f'"{profile.catalog}"."{profile.schema}"."{profile.table}"'
    fq_new      = f'"{profile.catalog}"."{profile.schema}"."{profile.table}_v2"'
    fq_bak      = f'"{profile.catalog}"."{profile.schema}"."{profile.table}_pre_repartition"'
    fq_old_meta = f'"{profile.catalog}"."{profile.schema}"."{profile.table}$files"'
    fq_new_meta = f'"{profile.catalog}"."{profile.schema}"."{profile.table}_v2$files"'
    bbox_col    = profile.bbox_col or "__geom_bbox__"
    z2_col      = profile.z2_col or "__geom_z2__"
    z2_trunc    = f"{z2_col}_trunc"
    arr         = render_partitioning_array(profile, grain, n)
    env_minx, env_miny, env_maxx, env_maxy = representative_envelope(profile, envelope_deg)
    parts = []

    writer_count = _writer_count_for(profile, grain, n)
    total_parts  = partition_count_for(profile, grain, n)
    parts.append("### Step 0 — Session properties\n")
    parts.append(
        "Run these in the same `trino>` session that will issue the CTAS; the "
        "properties stick for the duration of the session.\n")
    parts.append(
        f"**Write-side gotcha:** `iceberg.max_partitions_per_writer` is hard-"
        f"capped at 100 and can't be raised. The CTAS hash-distributes this "
        f"spec's **{total_parts:,} partitions** across `task_max_writer_count` "
        f"writers, so each writer must end up with ≤ 100. "
        f"`{total_parts:,} / 100 → {writer_count}` (next power of 2) is set "
        f"below. Sorting the input doesn't help — Trino drops `ORDER BY` and "
        f"`sorted_by` from a CTAS plan, so writer count is the only lever.\n")
    if writer_count >= 64 and total_parts > 6400:
        parts.append(
            "> ⚠ This spec needs more than 64 writers to stay under the cap, "
            "which is impractical on a single node (each writer holds open "
            "partition buffers). Load in batches instead — e.g. several "
            "`INSERT INTO … SELECT … WHERE` statements each scoped to a slice "
            "of the z2 keyspace so each writes ≤ 100 partitions.\n")
    elif writer_count >= 32:
        parts.append(
            f"> ⚠ {writer_count} writers on a single node holds a lot of "
            f"concurrent partition buffers open. Watch peak memory via "
            f"`system.runtime.tasks`; if it climbs toward the heap, drop "
            f"`parquet_writer_block_size` further or load in batches.\n")
    parts.append("```sql")
    parts.append("SET SESSION query_max_memory = '20GB';")
    parts.append("SET SESSION query_max_memory_per_node = '20GB';")
    parts.append("SET SESSION query_max_execution_time = '6h'; -- overrides any 60s cap from a prior monitor session")
    parts.append(f"SET SESSION iceberg.target_max_file_size = '{target_file_mb}MB';")
    parts.append(f"SET SESSION task_max_writer_count = {writer_count};")
    parts.append("SET SESSION iceberg.parquet_writer_block_size = '16MB';")
    parts.append("```\n")

    parts.append("### Step 1 — CTAS to the new table\n")
    parts.append(
        "Creates the rewritten table with the recommended partition spec. "
        "Target file size comes from the session property set in Step 0. "
        "Compression inherits from the iceberg catalog's config "
        "(typically `zstd`) — Trino blocks `write.*` keys in `extra_properties`, "
        "so they can't be set inline here. `object_store_layout_enabled = true` is "
        "the native Trino property for Iceberg `write.object-storage.enabled`: it "
        "hashes data-file paths across S3 prefixes so a large table's writes/reads "
        "spread over many partitions instead of throttling on one.\n")
    parts.append("```sql")
    parts.append(f"CREATE TABLE {fq_new}")
    parts.append(f"WITH (")
    parts.append(f"    partitioning = {arr},")
    parts.append(f"    format = 'PARQUET',")
    parts.append(f"    format_version = 2,")
    parts.append(f"    object_store_layout_enabled = true")
    parts.append(f") AS")
    parts.append(f"SELECT * FROM {fq_old};")
    parts.append("```\n")

    parts.append("### Step 2 — Row count parity check\n")
    parts.append(
        "Confirms the CTAS preserved every row. The two counts must match "
        "exactly before swapping table names.\n")
    parts.append("```sql")
    parts.append(f"SELECT (SELECT count(*) FROM {fq_old})  AS old_count,")
    parts.append(f"       (SELECT count(*) FROM {fq_new}) AS new_count;")
    parts.append("```\n")

    parts.append("### Step 3 — Confirm partition values are proper hex prefixes\n")
    parts.append(
        f"Expect values that look like {n}-char lowercase hex (e.g. `a62`, "
        f"`9f8`, `b04`). If you see decimal-formatted integers (`02`, `08`, "
        f"`10`), the writer is bypassing Iceberg's standard "
        f"`Truncate(string, N)` transform — the rewrite hasn't actually "
        f"fixed the spec and you need to address the writer before retrying.\n")
    parts.append("```sql")
    parts.append(f"SELECT DISTINCT partition.{z2_trunc} AS partition_value")
    parts.append(f"FROM {fq_new_meta}")
    parts.append(f"ORDER BY 1")
    parts.append(f"LIMIT 20;")
    parts.append("```\n")

    parts.append("### Step 4 — Verify spatial pruning improvement\n")
    parts.append(
        f"Counts files surviving a representative {envelope_deg}° envelope "
        f"predicate centered on the dataset's bbox "
        f"({env_minx:.2f}, {env_miny:.2f}, {env_maxx:.2f}, {env_maxy:.2f}). "
        f"Uses Iceberg's per-file bbox stats — metadata-only, runs in seconds, "
        f"no data scan. Run against both the NEW and OLD tables to see the "
        f"file-count reduction; a big drop is the pruning win.\n")
    parts.append(
        "If the bbox center isn't where your data is actually dense (common "
        "for globally-distributed bboxes that center on (0, 0)), edit the "
        "literals to a location you know is busy.\n")
    parts.append("**NEW table:**")
    parts.append("```sql")
    parts.append(f"SELECT count(*) AS files_surviving")
    parts.append(f"FROM {fq_new_meta}")
    parts.append(f"WHERE cast(json_extract_scalar(readable_metrics, '$[\"{bbox_col}.xmax\"].upper_bound') AS double) >= {env_minx}")
    parts.append(f"  AND cast(json_extract_scalar(readable_metrics, '$[\"{bbox_col}.xmin\"].lower_bound') AS double) <= {env_maxx}")
    parts.append(f"  AND cast(json_extract_scalar(readable_metrics, '$[\"{bbox_col}.ymax\"].upper_bound') AS double) >= {env_miny}")
    parts.append(f"  AND cast(json_extract_scalar(readable_metrics, '$[\"{bbox_col}.ymin\"].lower_bound') AS double) <= {env_maxy};")
    parts.append("```")
    parts.append("**OLD table (for comparison):**")
    parts.append("```sql")
    parts.append(f"SELECT count(*) AS files_surviving")
    parts.append(f"FROM {fq_old_meta}")
    parts.append(f"WHERE cast(json_extract_scalar(readable_metrics, '$[\"{bbox_col}.xmax\"].upper_bound') AS double) >= {env_minx}")
    parts.append(f"  AND cast(json_extract_scalar(readable_metrics, '$[\"{bbox_col}.xmin\"].lower_bound') AS double) <= {env_maxx}")
    parts.append(f"  AND cast(json_extract_scalar(readable_metrics, '$[\"{bbox_col}.ymax\"].upper_bound') AS double) >= {env_miny}")
    parts.append(f"  AND cast(json_extract_scalar(readable_metrics, '$[\"{bbox_col}.ymin\"].lower_bound') AS double) <= {env_maxy};")
    parts.append("```\n")

    parts.append("### Step 5 — Swap table names\n")
    parts.append(
        "Two separate `ALTER TABLE` statements — Trino doesn't support a "
        "single atomic multi-rename. There's a brief window between the two "
        "where neither name points at the new table; expect this to be "
        "sub-second in practice, but coordinate with any live consumers.\n")
    parts.append("```sql")
    parts.append(f"ALTER TABLE {fq_old} RENAME TO {fq_bak};")
    parts.append(f"ALTER TABLE {fq_new} RENAME TO {fq_old};")
    parts.append("```\n")

    parts.append("### Step 6 — Drop the old table (after verifying)\n")
    parts.append(
        "Run your benchmark / read queries against the renamed table first. "
        "Once you've confirmed counts and spatial pruning behavior look "
        "right, reclaim the storage:\n")
    parts.append("```sql")
    parts.append(f"DROP TABLE {fq_bak};")
    parts.append("```\n")

    parts.append("### Optional — Monitor progress while Step 1 runs\n")
    parts.append(
        "**⚠ Open a SEPARATE `trino>` session for this** — do not paste "
        "these SETs into the session running the CTAS. The `60s` execution "
        "limit below is meant to keep an *accidental* big monitor query "
        "from competing with the CTAS; running it in the CTAS session "
        "kills the CTAS at 60 seconds (`Query exceeded the maximum "
        "execution time limit of 60.00s`). The CTAS session should already "
        "have `query_max_execution_time = '6h'` from Step 0, which "
        "overrides this if you cross-contaminate — but starting fresh "
        "is safer.\n")
    parts.append("```sql")
    parts.append("SET SESSION query_max_memory = '2GB';")
    parts.append("SET SESSION query_max_memory_per_node = '2GB';")
    parts.append("SET SESSION query_max_execution_time = '60s';")
    parts.append("```\n")
    parts.append(
        "Aggregate per-task progress (rows / bytes / split state) across "
        "the running CTAS. The progress data lives in `system.runtime.tasks` "
        "as discrete columns — `system.runtime.queries` only carries the "
        "high-level state.\n")
    parts.append("```sql")
    parts.append("WITH running_ctas AS (")
    parts.append("    SELECT query_id, created")
    parts.append("    FROM system.runtime.queries")
    parts.append(f"    WHERE query LIKE '%{profile.table}_v2%'")
    parts.append("      AND state = 'RUNNING'")
    parts.append(")")
    parts.append("SELECT q.query_id,")
    parts.append("       date_diff('second', q.created, now()) AS sec_running,")
    parts.append("       sum(t.completed_splits)       AS completed_splits,")
    parts.append("       sum(t.running_splits)         AS running_splits,")
    parts.append("       sum(t.queued_splits)          AS queued_splits,")
    parts.append("       sum(t.processed_input_rows)   AS rows_processed,")
    parts.append("       sum(t.processed_input_bytes)  AS bytes_processed,")
    parts.append("       sum(t.physical_written_bytes) AS bytes_written")
    parts.append("FROM running_ctas q")
    parts.append("JOIN system.runtime.tasks t USING (query_id)")
    parts.append("GROUP BY q.query_id, q.created;")
    parts.append("```")

    return "\n".join(parts)


def report(profile: TableProfile, candidates: list[Candidate],
            target_partition_mb: int, envelope_deg: float,
            step_by_step: bool = False, target_file_mb: int = 128) -> str:
    out = []
    fq = f"{profile.catalog}.{profile.schema}.{profile.table}"
    out.append(f"# Partition recommendation: `{fq}`\n")
    out.append("## Profile\n")
    out.append(f"- **Rows**: {profile.total_rows:,}")
    out.append(f"- **Files**: {profile.file_count:,} (avg {fmt_bytes(profile.avg_file_bytes)})")
    out.append(f"- **Bytes per row**: {profile.bytes_per_row:.1f}")
    out.append(f"- **Geometry column**: `{profile.geom_col}` "
               f"(partition column: `{profile.z2_col or '(none)'}`, "
               f"kind: `{profile.z2_kind or 'none'}`)")
    out.append(f"- **Bbox**: x ∈ [{profile.bbox[0]:.3f}, {profile.bbox[2]:.3f}], "
               f"y ∈ [{profile.bbox[1]:.3f}, {profile.bbox[3]:.3f}]")
    if profile.time_col and profile.time_range:
        t0, t1 = profile.time_range
        span_days = (t1 - t0).total_seconds() / 86400
        out.append(f"- **Time column**: `{profile.time_col}` "
                   f"({t0} → {t1}, span {span_days:.1f} days)")
    else:
        out.append(f"- **Time column**: (none — no temporal partition will be recommended)")
    out.append(f"- **Current partitioning**: `{profile.current_spec}`\n")
    out.append(f"## Target\n")
    out.append(f"- **partition size** ≈ {target_partition_mb} MB  (drives scoring — bounds bytes-read per pruned partition)")
    out.append(f"- **file size** ≈ {target_file_mb} MB  (emitted into migration SQL as `iceberg.target_max_file_size`)")
    out.append(f"- representative query envelope = {envelope_deg}° × {envelope_deg}°\n")

    total_table_bytes = profile.total_rows * profile.bytes_per_row
    target_bytes = target_partition_mb * (1 << 20)
    if total_table_bytes < target_bytes:
        out.append(
            f"> **⚠ Small table notice**: the whole table is {fmt_bytes(total_table_bytes)}, "
            f"smaller than one target partition ({target_partition_mb} MB). Partitioning won't help "
            f"performance at this scale — any spec produces partitions much smaller than ideal. "
            f"Treat the recommendation below as the *future-proof* shape for when this table "
            f"grows; for now, a single-partition layout `ARRAY[]` would also be fine.\n"
        )

    if not candidates:
        out.append("**No viable candidates** — dataset may be too small or too sparse.\n")
        return "\n".join(out)

    out.append("## Top recommendations\n")
    # Distribution source: 'data' = per-file z2 stats from $files (approximate
    # when files are loosely sorted), 'exact' = GROUP BY scan (--exact-skew),
    # 'bbox' = uniform-over-bbox fallback (used when neither is available, e.g.
    # XZ2 or stats-less tables). Different warnings fire for each.
    has_real_data = any(c.distribution_source in ("data", "exact") for c in candidates[:5])
    out.append("| Rank | Grain | Trunc N | Time buckets | Populated cells | Total partitions | Mean bytes/part | **Max bytes/part** | Skew (max÷mean) | Pruning |")
    out.append("|---|---|---|---|---|---|---|---|---|---|")
    for i, c in enumerate(candidates[:5], 1):
        skew_str = "—" if c.distribution_source == "bbox" else f"{c.skew_ratio:.1f}×"
        out.append(
            f"| {i} | "
            f"{c.grain or '(none)'} | "
            f"{c.truncate_n or '(none)'} | "
            f"{c.time_buckets:,} | "
            f"{c.spatial_cells_populated:,} | "
            f"{c.total_partitions:,} | "
            f"{fmt_bytes(c.bytes_per_partition)} | "
            f"**{fmt_bytes(c.bytes_per_partition_max)}** | "
            f"{skew_str} | "
            f"{c.pruning_rate * 100:.1f}% |"
        )
    out.append("")
    out.append(
        f"_**Populated cells** are spatial cells the data actually occupies (counted from "
        f"per-file z2 stats), not the bbox-extent-only theoretical cell count. "
        f"**Max bytes/part** is the largest single partition's file size — the spec is scored "
        f"against this, not the mean, because a 10× hot partition kills file-pruning's "
        f"benefit (you still read that one giant file for every query that touches it). "
        f"**Skew** flags how lopsided the data is: 1.0× = uniform, 10× = one cell holds 10× "
        f"the mean, 100×+ = severely clustered. Pruning rate is the fraction of partitions "
        f"a {envelope_deg}° × {envelope_deg}° query within one time bucket would skip._\n")

    if not has_real_data:
        out.append(
            "> ⚠ `$files` did not expose z2 column statistics on this table — fell back to "
            "bbox-derived estimates that assume uniform spatial distribution. Predicted "
            "Max/Mean/Skew may be optimistic if the data is clustered.\n")

    # Loose-source warning: when files cover many truncate-N prefixes each, the
    # uniform-spread approximation smears row counts and *under*-counts the hot
    # partition. Direct user to --exact-skew for ground truth.
    best_for_warn = candidates[0]
    if best_for_warn.distribution_source == "data" and best_for_warn.max_file_span_n > 10:
        out.append(
            f"> ⚠ **Source files are loosely z2-sorted at N={best_for_warn.truncate_n}**: "
            f"the largest file's z2 bounds span **{best_for_warn.max_file_span_n} "
            f"truncate-{best_for_warn.truncate_n} prefixes**. The per-prefix histogram is "
            f"computed by uniformly spreading each file's rows across the prefixes its z2 "
            f"span covers — when files have wide spans this **smears the hot partition** "
            f"across many cells, so the printed *Max bytes/part* is an **under**-estimate "
            f"(actual hot partition may be 5–10× larger than shown). For a precise answer "
            f"re-run with **`--exact-skew`** (runs a single `GROUP BY truncate(z2, N)` scan "
            f"— O(rows), takes seconds on small tables, minutes on multi-billion-row ones).\n")
    elif best_for_warn.distribution_source == "exact":
        out.append(
            "> ℹ Used `--exact-skew`: distribution stats below come from a direct "
            "`GROUP BY` scan of the data — precise per-prefix row counts, no spread "
            "approximation.\n")

    best = candidates[0]
    out.append("## Recommended spec\n")
    out.append("```sql")
    out.append(f"partitioning = {render_partitioning_array(profile, best.grain, best.truncate_n)}")
    out.append("```\n")

    out.append("### Why this candidate")
    out.append(
        f"- **Max bytes per partition ({fmt_bytes(best.bytes_per_partition_max)})** lands "
        f"closest to the {target_partition_mb} MB target. Mean is {fmt_bytes(best.bytes_per_partition)}.")
    out.append(
        f"- {best.total_partitions:,} total partitions across "
        f"{best.time_buckets:,} time buckets × {best.spatial_cells_populated:,} "
        f"populated spatial cells (out of {best.spatial_cells_active:,} cells the bbox "
        f"would theoretically cover — populated cells are read from per-file `$files` z2 "
        f"stats).")
    if best.truncate_n:
        out.append(
            f"- At N={best.truncate_n} ({4 * best.truncate_n} effective bits) each spatial "
            f"cell is ~{360 / per_axis_cells(best.truncate_n):.2f}° lon × "
            f"~{180 / per_axis_cells(best.truncate_n):.2f}° lat. "
            f"Globally reachable: {best.spatial_cells_reachable:,} cells.")
    if best.distribution_source == "data" and best.skew_ratio >= 5.0:
        out.append(
            f"- **Data skew is {best.skew_ratio:.1f}× (max/mean).** The largest cell holds "
            f"{best.skew_ratio:.1f}× the average — this dataset is clustered, not uniform. "
            f"Scoring uses the *max* partition size precisely because a 10× hot partition "
            f"defeats file pruning: queries that intersect that cell still pay to scan its "
            f"oversized file. A coarser N would inflate this further; a finer N would split "
            f"the hot cell at the cost of more total partitions.")
    out.append(
        f"- A representative {envelope_deg}° tight query would scan "
        f"{best.sample_query_partitions} of {best.total_partitions:,} partitions "
        f"({best.pruning_rate * 100:.1f}% pruned).\n")

    # Write-side feasibility: Trino caps open writers at 100 per writer, and
    # that cap can't be raised. Warn when the recommended spec needs a high
    # writer count (or batching) to load in a single CTAS.
    writer_count = _writer_count_for(profile, best.grain, best.truncate_n)
    out.append("### Write-side partition limit\n")
    out.append(
        f"Trino's `iceberg.max_partitions_per_writer` is hard-capped at 100 "
        f"(not raisable). A CTAS hash-distributes this spec's "
        f"{best.total_partitions:,} partitions across `task_max_writer_count` "
        f"writers, so that knob must be set to **{writer_count}** "
        f"(`ceil({best.total_partitions:,}/100)` rounded up to a power of 2) "
        f"for the load to succeed. The migration SQL sets this for you.")
    if writer_count >= 64 and best.total_partitions > 6400:
        out.append(
            "**This exceeds what a single-node CTAS can comfortably handle.** "
            "Load in batches (multiple `INSERT … WHERE` statements over slices "
            "of the z2 keyspace, each writing ≤ 100 partitions), or step down "
            "to a coarser `--target-partition-mb` / smaller N to reduce the partition "
            "count. On a multi-node Spark cluster, `RewriteDataFiles` sidesteps "
            "this entirely.")
    elif writer_count >= 32:
        out.append(
            f"At {writer_count} writers, single-node memory pressure from "
            f"concurrent open-partition buffers is a real risk — watch peak "
            f"memory during the load. A coarser spec (smaller N) would need "
            f"fewer writers; weigh that against the read-side pruning loss.")
    out.append("")

    if step_by_step:
        out.append("## Migration SQL — step by step\n")
        out.append(render_ctas(profile, best.grain, best.truncate_n,
                                envelope_deg=envelope_deg, step_by_step=True,
                                target_file_mb=target_file_mb))
        out.append("")
    else:
        out.append("## Migration SQL\n")
        out.append("```sql")
        out.append(render_ctas(profile, best.grain, best.truncate_n,
                                envelope_deg=envelope_deg,
                                target_file_mb=target_file_mb))
        out.append("```\n")

    out.append("## Caveats\n")
    # Sizing-based heads-up: at this row count, single-node Trino CTAS is hours.
    total_bytes = profile.total_rows * profile.bytes_per_row
    if total_bytes > 100 * (1 << 30):  # > 100 GB
        out.append(
            f"- **This table is {fmt_bytes(total_bytes)} on disk.** A Trino CTAS rewrites every "
            f"row through Trino's writer + S3 — on a single-node Trino expect hours, not minutes. "
            f"At this scale prefer Spark's `RewriteDataFiles` action: it parallelizes across "
            f"workers and runs in 10s of minutes against the same Iceberg table.")
    out.append(
        "- **Files vs splits in the prediction**: the analyzer estimates partition count, which "
        "drives file count. Trino's Parquet reader may split a single file into multiple "
        "row-group-sized scan units, so the *splits* number in EXPLAIN ANALYZE output will be "
        "larger than the file count. The verify-prediction query at step 4 of the migration "
        "SQL gives the file-level number, which is what bounds I/O.")
    out.append(
        "- **Populated-cell counts come from `$files` z2 statistics** (per-file lower/upper "
        "z2 bounds + record_count), with each file's rows spread uniformly across the "
        "truncate-N prefixes its z2 span covers. This is exact when files are tightly z2-"
        "sorted and conservatively over-counts populated cells when they aren't — pushing "
        "the recommendation slightly *finer* than strictly necessary in the loose case. If "
        "you compact / sort the source by z2 first, re-running this skill may recommend a "
        "coarser N.")
    out.append(
        "- **Sanity-check the partition values after rewrite** (step 3 of the migration SQL). "
        "Iceberg's `truncate(string, N)` is well-defined, but writer implementations have "
        "historically deviated — emitting decimal-formatted integers or other non-conforming "
        "values that break predicate pushdown silently. If step 3 shows partition values that "
        "don't look like N-char prefixes of the source column, the writer is the problem, not "
        "the spec.")
    out.append(
        "- **The recommendation doesn't factor in query-shape mix.** It optimizes for the "
        "representative envelope size (--query-envelope-deg, default 1.0°). If your workload "
        "is dominated by wide-area scans rather than tight envelopes, coarser N may be better "
        "(fewer manifests opened per query); re-run with a larger --query-envelope-deg to see "
        "how the ranking shifts.")
    return "\n".join(out)


# ── Main ──────────────────────────────────────────────────────────────────────

def die(msg: str, code: int = 1):
    sys.stderr.write(msg + "\n")
    sys.exit(code)


def parse_table(spec: str) -> tuple[str, str, str]:
    parts = spec.split(".")
    if len(parts) != 3:
        die(f"Expected <catalog>.<schema>.<table>, got: {spec!r}")
    return parts[0], parts[1], parts[2]


class HelpfulParser(argparse.ArgumentParser):
    """Argparse subclass that prints full --help on any usage error (including
    missing positional). Default argparse prints only a terse one-liner, which
    leaves the user with no clue what flags exist."""

    def error(self, message: str) -> None:
        sys.stderr.write(f"error: {message}\n\n")
        self.print_help(sys.stderr)
        sys.exit(2)


def _emit_script(script_text: str, out_path: Optional[str]) -> None:
    """Write the migration script to disk + chmod +x, or fall back to stdout
    with a nag about making the redirected file executable. Used by both the
    default --script path (analyzer picks the spec) and the --spec override
    path (user picks the spec)."""
    if out_path:
        with open(out_path, "w") as f:
            f.write(script_text)
            if not script_text.endswith("\n"):
                f.write("\n")
        # Set executable bit for owner/group/other (rwxr-xr-x). Generated
        # scripts are meant to run; making the user `chmod +x` is friction.
        os.chmod(out_path, 0o755)
        # For relative paths, suggest `./script.sh`; for absolute paths,
        # `./` is wrong — invoke the absolute path directly.
        run_cmd = out_path if os.path.isabs(out_path) else f"./{out_path}"
        sys.stderr.write(f"✓ Wrote executable migration script to {out_path}\n")
        sys.stderr.write(f"  Run it with: {run_cmd}\n")
    else:
        sys.stdout.write(script_text)
        if not script_text.endswith("\n"):
            sys.stdout.write("\n")
        sys.stderr.write("\n")
        sys.stderr.write("⚠ Script written to stdout. If you redirected to a file, make it executable:\n")
        sys.stderr.write("    chmod +x <your-file>.sh\n")
        sys.stderr.write("  (or re-run with --out <path> to write + chmod automatically)\n")


def main():
    p = HelpfulParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("table", help="Fully-qualified Iceberg table: catalog.schema.table")
    p.add_argument("--host", default=os.environ.get("TRINO_HOST", "localhost"),
                   help="Trino host (default: $TRINO_HOST or localhost)")
    p.add_argument("--port", type=int, default=int(os.environ.get("TRINO_PORT", "8080")),
                   help="Trino port (default: $TRINO_PORT or 8080)")
    p.add_argument("--target-partition-mb", type=int, default=128,
                   help="Target total bytes PER PARTITION in MB — drives the (grain, truncate_N) "
                        "scoring. A query that prunes to a partition reads ALL files in it, so "
                        "this bounds per-touched-partition I/O. Bigger target → coarser spec, "
                        "fewer/larger partitions. Default: 128. NOTE: this flag was previously "
                        "named --target-file-mb (which conflated 'partition size' with 'file size'); "
                        "if you got an unrecognized-arg error, that's the rename.")
    p.add_argument("--target-file-mb", type=int, default=128,
                   help="Target bytes per Parquet file in MB — written into the migration SQL's "
                        "`iceberg.target_max_file_size` session property. Controls the writer's "
                        "row-group/file slicing inside a partition; doesn't affect the analyzer's "
                        "scoring. Default: 128.")
    p.add_argument("--query-envelope-deg", type=float, default=1.0,
                   help="Representative query envelope side length, in degrees (default: 1.0)")
    p.add_argument("--steps", action="store_true",
                   help="Render the migration SQL as separate step-by-step sections with prose between each "
                        "(easier to copy-paste from CLI pagers that truncate long markdown blocks).")
    p.add_argument("--script", action="store_true",
                   help="Instead of the report, emit a restartable bash migration script that creates the "
                        "destination table and loads it via DATA-BALANCED batched INSERTs. Reads the z2 "
                        "distribution from Iceberg $files metadata (O(files), no data scan), then cuts "
                        "variable-width z2 ranges that each hold a bounded, even amount of data (so skewed "
                        "data can't OOM one batch). Re-runnable: skips batches already committed. Pair with "
                        "--out to write+chmod the script, or redirect stdout to a file (then chmod +x it).")
    p.add_argument("--out", metavar="PATH",
                   help="With --script, write the script to PATH and chmod +x it (no stdout output). "
                        "Without this flag, --script prints to stdout and you must chmod the redirected "
                        "file yourself.")
    p.add_argument("--exact-skew", action="store_true",
                   help="Replace the $files-metadata histogram (O(files), approximate when source is loosely "
                        "z2-sorted) with a precise GROUP BY truncate(z2, N) scan against the data. O(rows) — "
                        "seconds on 10^7-row tables, minutes on multi-billion-row ones. Use when the "
                        "default report shows a loose-source warning and you want a precise Max bytes/part.")
    p.add_argument("--no-cache", action="store_true",
                   help="Skip the on-disk cache lookup AND don't write results to it. Use when you "
                        "suspect the cache is stale (it auto-invalidates on table writes via snapshot ID, "
                        "but this is a manual override).")
    p.add_argument("--cache-dir", metavar="PATH", default=str(DEFAULT_CACHE_DIR),
                   help=f"On-disk cache directory. Cache key includes the table's current Iceberg "
                        f"snapshot ID, so table mutations auto-invalidate. (default: {DEFAULT_CACHE_DIR})")
    p.add_argument("--spec", metavar="<grain>+<N>",
                   help="Override the analyzer's recommendation: use this partition spec directly "
                        "instead of ranking candidates. Format: '<grain>+<N>' (e.g. 'day+3', "
                        "'month+4', 'year+5') or '+<N>' for spatial-only. Skips candidate evaluation "
                        "(no GROUP BYs, no metadata smearing) and goes straight to migration "
                        "generation — useful when you already know what you want and just need the "
                        "script. Currently supported only with --script.")
    args = p.parse_args()

    if args.out and not args.script:
        die("--out only makes sense with --script (it controls where the script file is written).")

    catalog, schema, table = parse_table(args.table)
    try:
        conn = connect(args.host, args.port)
        profile = discover(conn, catalog, schema, table)
    except Exception as e:
        die(f"Trino connection or discovery failed: {e}")

    # Enable on-disk caching unless explicitly disabled. Snapshot ID lookup is
    # one cheap query; if it fails (empty table, no perms), caching is silently
    # skipped — the fetches just run as before.
    if not args.no_cache:
        snap_id = _fetch_snapshot_id(conn, catalog, schema, table)
        if snap_id:
            object.__setattr__(profile, "_cache_dir", Path(args.cache_dir))
            object.__setattr__(profile, "_snapshot_id", snap_id)

    # --spec override: parse "<grain>+<N>" (e.g. "day+3") and skip candidate
    # ranking entirely. Only the migration-script path uses this; ranking is
    # needed for the report's comparison table.
    if args.spec:
        if not args.script:
            die("--spec currently only works with --script. The report path needs "
                "candidate ranking to populate the comparison table; --spec is a "
                "fast-path that skips ranking.")
        spec_raw = args.spec.strip()
        if "+" not in spec_raw:
            die(f"--spec must contain '+' (e.g. 'day+3' or '+5'); got: {args.spec!r}")
        grain_str, n_str = spec_raw.rsplit("+", 1)
        spec_grain = grain_str.strip() or None
        try:
            spec_n = int(n_str)
        except ValueError:
            die(f"--spec N must be an integer; got: {n_str!r}")
        valid_grains = [g[0] for g in GRAINS]
        if spec_grain and spec_grain not in valid_grains:
            die(f"--spec grain must be one of {valid_grains} (or empty); got: {spec_grain!r}")
        if spec_n not in N_CANDIDATES:
            sys.stderr.write(f"⚠ --spec N={spec_n} is outside default candidates "
                             f"{N_CANDIDATES}; proceeding anyway\n")
        if not profile.z2_col:
            die("--spec requires a spatial z2/xz2 partition column on the table.")
        sys.stderr.write(f"→ --spec override: using grain={spec_grain!r} truncate_n={spec_n} "
                         f"(skipping candidate ranking)\n")
        script_text = render_migration_script(conn, profile, spec_grain, spec_n,
                                               target_file_mb=args.target_file_mb)
        _emit_script(script_text, args.out)
        return

    candidates = rank_candidates(conn, profile, args.target_partition_mb,
                                  args.query_envelope_deg, exact_skew=args.exact_skew)
    if not candidates:
        die("No viable partition candidates — table may be too small or lack a partitionable geom column.")
    best = candidates[0]

    if args.script:
        if not (best.truncate_n and profile.z2_col):
            die("--script needs a spatial z2/xz2 partition column to batch on; "
                "this table's best candidate has no spatial dimension.")
        # Reads the distribution from Iceberg $files metadata — O(files), no data scan.
        script_text = render_migration_script(conn, profile, best.grain, best.truncate_n,
                                               target_file_mb=args.target_file_mb)
        _emit_script(script_text, args.out)
        return

    print(report(profile, candidates, args.target_partition_mb, args.query_envelope_deg,
                  step_by_step=args.steps, target_file_mb=args.target_file_mb))


if __name__ == "__main__":
    main()
