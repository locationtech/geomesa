---
name: optimize-partitioning
description: Recommend a temporal + Z2/XZ2 partition spec for an Iceberg table by running diagnostic queries via Trino. Advisory only — prints analysis and migration SQL; never executes ALTER/CTAS itself. Use when the user asks to tune partitioning for a specific table, or after benchmarks show spatial pruning isn't pulling its weight.
---

# optimize-partitioning

Inspect an Iceberg table that follows this repo's spatial column conventions
(`geom` + `__geom_bbox__` + `__geom_z2__` / `__geom_xz2__`, optional `dtg` /
other timestamp column) and recommend a partition spec sized for the actual
data volume, spatial extent, and time range. Outputs a written analysis plus
the SQL needed to migrate.

## When to use

- User asks to "optimize partitioning", "pick the right truncate width", "tune
  the partition spec", or similar against a specific catalog/schema/table.
- After diagnostic work shows the current partition spec is producing too many
  tiny files or doing no pruning (e.g. `truncate(__geom_z2__, 1)` against
  globally-distributed data).
- After ingest of a new dataset where the right spec wasn't known up-front.
- Especially useful for **spatially clustered datasets** (taxi GPS in one city,
  AIS along coastlines, flight tracks over CONUS) — the analyzer's skew-aware
  scoring catches the "93% of files pruned, 99% of rows still read" failure
  mode that bbox-derived planning misses.

Don't use this skill for:

- Tables without the spatial-column convention (no `__<X>_bbox__` /
  `__<X>_z2__` / `__<X>_xz2__`). The recommendation logic assumes those exist.
- Tables that aren't reachable from the Trino instance you have access to.

## How to invoke

Run the analyzer with the project venv (so the `trino` client is on path):

```bash
tools/.venv/bin/python .claude/skills/optimize-partitioning/analyze.py \
  <catalog>.<schema>.<table> \
  [--host localhost] [--port 8080] \
  [--target-partition-mb 128] [--target-file-mb 128] [--query-envelope-deg 1.0] \
  [--steps] [--exact-skew] [--script [--out PATH]]
```

Run the analyzer with no arguments (or `--help`) for a worked-example help
screen with copy-pasteable invocations for each common mode.

If `tools/.venv` doesn't exist yet, run `make install-trino` once. The analyzer
never writes to the table — it only runs `SELECT`, `DESCRIBE`, and
`SHOW CREATE TABLE`.

The `--steps` flag re-renders the migration SQL block as separate H3 sections
(one per step) with prose explanation between code blocks. Use it when the
inline-comment layout gets truncated by a CLI pager or when the user wants
to copy-paste step-by-step into a `trino>` session.

The `--exact-skew` flag swaps the default `$files`-metadata histogram (fast,
O(files); approximate when files are loosely z2-sorted) for a precise
`GROUP BY truncate(z2, N)` scan against the data (slower, O(rows); seconds
on 10⁷-row tables, minutes on multi-billion-row tables). Use it when the
default report shows a **loose-source warning** (top of recommendations
table) — that warning means the per-prefix histogram is computed by
smearing each file's rows across a wide z2 span, which *under*-estimates
the hot partition. `--exact-skew` gives ground truth.

The `--script` flag emits a **restartable bash migration script** instead of
the report. Pair it with `--out PATH` so the analyzer writes the file and
chmods it `+x` for you; without `--out` the script goes to stdout and the
user has to make it executable themselves. To build it, `--script` reads the
z2 distribution from Iceberg `$files` metadata (`record_count` + per-file z2
bounds — O(files), seconds, no data scan), cuts **data-balanced z2 ranges**, and
bakes them into the script. The script creates the destination table and loads
it via batched INSERTs over those ranges. See "Batched, restartable migration"
below for why balancing by real data beats a fixed split for high-partition-count
specs.

## What the analyzer does

1. **Discovers** the geometry column, bbox/z2/xz2 companions, and time column
   from `DESCRIBE`. Identifies whether the table is point-data (Z2) or
   envelope-data (XZ2) by looking at which companion exists.
2. **Profiles** the data via diagnostic queries:
   - Total row count, spatial bbox extent, temporal range
   - Per-file size + record count from `<table>$files` → bytes per row
   - Current partition spec via `SHOW CREATE TABLE`
3. **Reads the actual z2 distribution from `$files` metadata**
   (`record_count` + per-file z2 lower/upper bounds). For each candidate
   truncate width N, spreads each file's rows across the prefixes its z2
   span covers, producing a per-cell row-count histogram. Falls back to
   a bbox-uniform estimate when `$files` doesn't expose z2 stats (rare;
   typically only on XZ2 or pre-stats tables).
4. **Scores `(temporal_grain, truncate_N)` combos** by
   `|log2(max_bytes_per_partition / target)|` — i.e. how close the *hot*
   partition's file size is to the target. Ties broken by preferring fewer
   total partitions. **Max, not mean**, because file pruning rejecting 93%
   of files doesn't help if the surviving 7% holds 99% of the rows (the
   tdrive Beijing-cluster case).
5. **Emits**:
   - A markdown analysis block (data shape, current spec, recommendation,
     top alternatives table with columns *Mean bytes/part*, *Max bytes/part*,
     *Skew (max÷mean)*, *Populated cells*).
   - A **loose-source warning** (⚠) at the top of the recommendations table
     when source files cover >10 truncate-N prefixes each — the histogram
     under-estimates the hot partition in that case; offer `--exact-skew`.
   - The exact `partitioning = ARRAY[...]` line to use.
   - A "Write-side partition limit" note: the `task_max_writer_count` a single
     CTAS would need, and a warning when that's impractically high.
   - Inline CTAS migration SQL (or step-by-step with `--steps`). For the
     recommended apply path, `--script` emits a batched restartable bash
     script instead — see below.

## What to do with the output

**Render the analyzer's stdout into the chat verbatim.** The markdown report
(profile, recommendations table, recommended spec, migration SQL — including all
the per-step blocks when `--steps` is used) IS the deliverable. Do not summarize
it, abbreviate it, or replace it with a "here's what it recommended" paragraph.
The user invoked the skill to read that output; if they have to scroll through
tool transcripts to find it, the skill failed. Brief commentary after the report
is fine — but the report itself must appear in the assistant message.

Then **always offer both of these follow-ups**:

1. **A restartable migration script** (`--script --out PATH`). This is the
   recommended way to apply the recommendation for any spec with more than ~100
   partitions — which is almost all of them. It reads the z2 distribution from
   `$files` metadata (O(files) — seconds even on a multi-billion-row table).
   Use `--out` so the analyzer writes the file and sets the executable bit for
   you (otherwise the script goes to stdout and the user has to `chmod +x` it):

   ```bash
   tools/.venv/bin/python .claude/skills/optimize-partitioning/analyze.py \
     <catalog>.<schema>.<table> --script --out migrate_<table>.sh
   ```

   Stdout redirection still works (`--script > migrate.sh`) but then the user
   has to `chmod +x` the file before running it — prefer `--out`.

   See "Batched, restartable migration" below for what it does and why
   balancing by real data beats a single CTAS (or a fixed-width split).

2. **Step-by-step SQL** (`--steps`) for users who'd rather run the migration
   by hand in a `trino>` session and review output between steps. The default
   inline SQL block is compact but `trino>` pagers sometimes truncate it.

   ```bash
   tools/.venv/bin/python .claude/skills/optimize-partitioning/analyze.py \
     <catalog>.<schema>.<table> --steps
   ```

**If the report shows a `⚠ Source files are loosely z2-sorted` warning**, also
offer to re-run with `--exact-skew` — the default histogram under-estimates the
hot partition on loose sources (typically Trino-CTAS-written tables), so the
recommendation may be biased finer than necessary. `--exact-skew` runs one
`GROUP BY truncate(z2, N)` scan against the data for ground-truth row counts.
Mention the cost: O(rows), seconds on small tables, minutes on multi-billion-row
ones. If the user is OK with that, re-run:

```bash
tools/.venv/bin/python .claude/skills/optimize-partitioning/analyze.py \
  <catalog>.<schema>.<table> --exact-skew
```

Other follow-ups worth mentioning:

- Explore alternatives if the recommended grain/N doesn't fit their workload
  (e.g. they expect bigger queries, prefer fewer files).
- Re-run with different `--target-partition-mb` (drives scoring → coarser/finer N)
  or `--target-file-mb` (writer file-slicing knob, independent of scoring), or
  with a different `--query-envelope-deg`, to see how
  the recommendation shifts.

**Do not run the migration yourself.** It's destructive (rename + drop), and the
user's environment (catalog permissions, ingest pipeline coordination, cost) is
theirs to evaluate. Generate the script/SQL; let them execute it.

## Batched, restartable migration (`--script`)

A single CTAS to a high-partition-count spec fails on Trino with *"Exceeded
limit of 100 open writers for partitions"* — `iceberg.max_partitions_per_writer`
is **hard-capped at 100 and cannot be raised**, and a CTAS hash-distributes all
partitions across the writers at once. (Sorting the input doesn't help — Trino
drops `ORDER BY` / `sorted_by` from a CTAS write plan.) Batching the load is the
way out, but **how you cut the batches matters**: an early version split on the
leading hex char of the z2 column (16 fixed buckets). That assumes the z2
keyspace is evenly populated. Spatial data is clustered (flight tracks over
CONUS, AIS along coastlines), so a few hex buckets carry most of the rows and
partitions — and that one oversized batch OOMs the writer or trips the cap.
**Don't split by a fixed key range; split by actual data.**

A full `GROUP BY truncate(z2,N)` scan would read the exact distribution, but on
a multi-billion-row table that's ~20 minutes — too slow for a planning step you
re-run while iterating. So `--script` reads **Iceberg `$files` metadata** instead
(`record_count` + per-file z2 lower/upper bounds — O(files), seconds), sorts
files by z2, and packs **variable-width, data-balanced batches** baked into the
script as an explicit `RANGES` array:

- **Each batch's partition count is hard-capped, not estimated.** File metadata
  can't tell a populated prefix from an empty one, and files overlap heavily
  when the source isn't tightly z2-sorted. So instead of estimating per-batch
  partitions (which over-counts wildly across overlapping files), the script
  spreads each file's rows across the prefixes its z2 span covers, walks
  prefixes in order, and cuts every `max_cells = mem_cap / time_buckets`
  **covered** prefixes (or sooner, on a ~1.5× row target), where `mem_cap =
  WRITE_BUFFER_BUDGET_MB / block_size` (default 4096/16 = 256) is the **write-
  buffer memory budget**. Because the truly populated prefixes are a subset of
  the covered ones, a batch holds at most `covered × buckets` partitions — a hard
  ceiling that holds across sparse gaps too (a raw range-width cap silently
  overshoots there). Dense regions get many narrow ranges; sparse regions merge
  into wide ones.
- **Two distinct heap regimes — and they pull writer count opposite ways.**
  (a) *Write-buffer* heap ≈ `open_partitions × block_size`, bounded by `mem_cap`
  and invariant to writer count. A batch sized to the writer cap alone (560
  prefixes at 8 writers ≈ 9GB of buffers) OOM'd a node; the `WRITE_BUFFER_BUDGET_MB`
  cap (256 ≈ 4GB) fixes that. (b) *Scan/exchange* heap **grows with
  `task_max_writer_count`** — each writer is another parallel read+shuffle
  pipeline. A 1.2B-row / **23-partition** batch (write buffers negligible) at **16
  writers** pinned a 24GB heap into a GC death-spiral; the same batch at 8 would
  have committed. So the script **derives `task_max_writer_count` as the *minimum*
  that clears the 100-open-partitions-per-writer hard cap** (`ceil(max_cells /
  70)`, power of 2 — typically 4) and **never raises it to go faster**. It also
  pins `query_max_memory_per_node` to ~half the node heap so one INSERT can't
  starve GC + untracked encode buffers. `WRITE_BUFFER_BUDGET_MB` and
  `PARQUET_WRITER_BLOCK_MB` are constants at the top of `analyze.py`; raising the
  budget raises both the batch size *and* the derived writer count together.
- **Batch predicates are z2 RANGES** (`__geom_z2__ >= 'a62' AND < 'b04'`),
  contiguous and non-overlapping, so they tile the keyspace exactly once.
  Iceberg's per-file z2 stats prune non-matching files **only as well as the
  source is z2-clustered** — if files each span a wide z2 range (loosely-sorted
  source), batches re-scan overlapping files and total I/O is several× one scan.
  The script detects this (a file's prefix span wider than a batch range) and
  prints a ⚠ NOTE; the fix is to compact/sort the source by z2 first. (GeoMesa
  writers that sort on write produce tight files and near-1× I/O.)
- **Restartable**: each batch is skipped if the destination already has rows in
  its z2 range. Iceberg INSERTs commit atomically — a killed or failed INSERT
  leaves zero rows — so "range already populated" reliably means "batch done."
  Re-running resumes from the first incomplete batch. No external state file;
  the destination table is the source of truth.
- The script finishes with a **row-count parity check** (source vs destination)
  and prints the rename statements to run once the user has validated queries.
  It does **not** rename or drop anything itself.

Caveats of the metadata estimate: row counts per batch are *approximate*
(uniform-within-file spreading smears a dense file's rows across its z2 span), so
actual per-batch rows can be less even than the printed `rows≈`. That only
affects how long each INSERT runs — the partition ceiling and correctness are
exact, because the run-time z2 predicate attributes every row to exactly one
batch. If batches still OOM, match the lever to the symptom:
- **GC death-spiral / worker `PAGE_TRANSPORT_TIMEOUT`** (heap pinned, GC reclaims
  <1%, the batch has *few* partitions but huge row volume) → **scan/exchange**
  heap. Lower `task_max_writer_count` (or `query_max_memory_per_node`), or split
  the batch by rows. Do **NOT** raise `task_max_writer_count` to go faster — more
  writers = more parallel scan pipelines = more heap; that is exactly what caused
  this failure (16 writers OOM'd what 8 would have survived).
- **"Exceeded limit of 100 open writers for partitions"** → too many partitions
  per writer. This is the *only* case where more writers helps; the derived count
  already clears it, so it shouldn't occur.
- **Write-buffer pressure** (batch has *many* partitions) → lower
  `WRITE_BUFFER_BUDGET_MB` or `PARQUET_WRITER_BLOCK_MB` for narrower batches.

The biggest single win for a loosely-z2-sorted source is **compacting/sorting it
by z2 first**: it both tightens pruning (less read amplification — the failed
batch read 2.1B rows to write 1.2B) and shrinks per-batch scan heap.

## Heuristics the analyzer uses

- **Z2/XZ2 choice**: presence of `__<X>_z2__` or `__<X>_xz2__` column. If both
  exist, XZ2 wins (handles non-point geometries correctly).
- **Spatial cell math** under the `<<2`-shifted, hex-encoded Z2 convention
  used by `tools/common.py::geom_z2_raw_hex`:
  - `truncate(__<X>_{z2,xz2}__, N)` → `4N` bits → `2^(2N)` cells per axis →
    cell width `360°/2^(2N)` lon × `180°/2^(2N)` lat.
  - The left-shift puts hemisphere bits in the top hex position, so all
    16 hex digits are reachable at every position; reachable = `16^N`.
- **Populated spatial cells** (skew-aware): read from per-file z2 stats in
  `<table>$files` (`record_count` + `readable_metrics.<z2_col>.lower_bound /
  upper_bound`). For each candidate N, the analyzer truncates each file's
  z2 lower/upper bounds to N hex chars and spreads the file's rows uniformly
  across the prefixes its z2 span covers. Falls back to bbox-derived
  uniform-distribution estimate if `$files` doesn't expose z2 column stats
  (flagged in the report).
- **Max bytes per partition** is the analyzer's primary score, **not** the
  mean. A spatially skewed dataset (e.g. taxi GPS clustered in one metro
  area) can show a small mean while one hot cell holds 100× the rows — that
  one giant file kills file-pruning's benefit because every query touching
  the hot cell still scans it. Scoring by max prefers specs that bound the
  hot partition; the **Skew** column (max/mean) makes the situation visible.
- **Temporal grain candidates**: `year`, `month`, `day`, `hour`. Adding a
  finer temporal grain is one way to break up a spatially-hot cell — slicing
  Beijing across 7 days produces 7 partitions instead of 1, dividing the
  hot cell's row count by ~7. The analyzer considers all (grain, N) combos.
- **Target bytes per partition** = `target_file_mb`. The analyzer ranks
  candidates by `|log2(max_bytes_per_partition / target_bytes)|`. Ties
  broken by preferring fewer total partitions.
- **Pruning rate** for a sample query envelope (default 1° × 1°): the fraction
  of partitions a representative tight query would NOT have to read.
- **Files vs splits**: the analyzer's "total partitions" estimate drives file
  count. Trino's Parquet reader can split one file into multiple row-group
  scan units, so `Splits: N` in EXPLAIN ANALYZE will exceed the file count by
  roughly the row-groups-per-file ratio (typically 1–4). The generated
  migration SQL includes a `$files`-metadata query that gives the authoritative
  file count.
- **Loosely-sorted source caveat**: the per-prefix row spread is exact when
  files are tightly z2-sorted (every file's z2 span maps to a small number
  of prefixes — PyIceberg ingest produces this shape, since partition writes
  emit one file per spatial cell). A loosely-sorted source — e.g. a fresh
  Trino CTAS — produces files that span wide z2 ranges (hundreds of N=4
  prefixes), and the uniform spread *smears* the hot cell's rows across all
  of them. The printed *Max bytes/part* is then an **under**-estimate: the
  recommendation looks fine on paper but the actual hot partition can be
  5–10× larger. The report flags this with a `⚠ Source files are loosely
  z2-sorted` warning naming the largest file's span. **Workaround**: re-run
  with `--exact-skew` for a precise GROUP BY scan, OR compact/sort the
  source by z2 first and re-run the default analyzer.

## Validating the prediction without rewriting

The migration script emits two metadata-only queries that let you verify the
expected pruning behavior before committing to a CTAS:

1. **Partition value sanity check** — `SELECT DISTINCT partition.<z2>_trunc
   FROM "<table>$files"` after the rewrite. Values should look like N-char
   lowercase hex prefixes of the source column (e.g. `a62`, `9f8`). If they
   look like decimal integers (`02`, `08`, `10`), the writer is bypassing
   Iceberg's `Truncate(string, N)` transform and the rewrite hasn't fixed
   the underlying problem.
2. **`$files` predicate query** — counts files surviving a representative
   tight bbox predicate using `readable_metrics` JSON. Run against both the
   OLD and NEW tables to see the actual file-count reduction. This is the
   single most reliable indicator of whether the rewrite is worth it; it
   runs in seconds on table metadata without scanning any data.

## CTAS vs Spark `RewriteDataFiles`

For tables under ~100 GB, the Trino CTAS in the migration script is fine.
Beyond that, single-node Trino reads + writes 100s of GB through one node's
CPU and S3 bandwidth — a multi-hour operation. Iceberg's
`SparkActions.rewriteDataFiles(table)` parallelizes across workers and
finishes in 10s of minutes against a moderately-sized cluster. The analyzer
flags this in its caveats when the table exceeds 100 GB.

## Files in this skill

- `SKILL.md` — this file
- `analyze.py` — the analyzer script (Python; uses `trino` client)
