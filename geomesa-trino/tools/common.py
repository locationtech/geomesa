import os
from dataclasses import dataclass
from typing import Callable, Iterable, Literal, Optional

import pyarrow as pa
from shapely import wkb as shapely_wkb
from shapely.geometry.base import BaseGeometry
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.types import FloatType, NestedField, StringType, StructType


SpatialIndex = Literal["z2", "xz2"]


def trino_host_port() -> tuple[str, int]:
    """Trino coordinator address; override via TRINO_HOST / TRINO_PORT env vars."""
    return (os.environ.get("TRINO_HOST", "localhost"),
            int(os.environ.get("TRINO_PORT", "8080")))


def trino_connect(user: str = "trino"):
    """DBAPI connection to the Trino coordinator at trino_host_port()."""
    import trino
    host, port = trino_host_port()
    return trino.dbapi.connect(host=host, port=port, user=user)


def local_rest_catalog(name: str = "default") -> RestCatalog:
    """RestCatalog pointed at the local docker-compose stack (iceberg-rest + MinIO).

    Single source of truth for the connection block every ingest/purge script
    shared verbatim. Credentials are the bundled local MinIO dev defaults — not
    secrets; the AWS stack is driven through the Trino catalog properties, not
    this PyIceberg path.
    """
    return RestCatalog(
        name=name,
        **{
            "uri": "http://localhost:8181",
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password",
            "s3.path-style-access": "true",
            "s3.region": "us-east-1",
            "warehouse": "s3://warehouse/",
        },
    )


def _spread_bits(x: int) -> int:
    """Z2SFC bit-spread: insert a 0 between every bit of `x`. Only the low 31
    bits of the input are considered (mirrors `Z2.split` in geomesa-z3:5.4.0)."""
    x &= 0x7FFFFFFF
    x = (x | (x << 16)) & 0x0000FFFF0000FFFF
    x = (x | (x <<  8)) & 0x00FF00FF00FF00FF
    x = (x | (x <<  4)) & 0x0F0F0F0F0F0F0F0F
    x = (x | (x <<  2)) & 0x3333333333333333
    x = (x | (x <<  1)) & 0x5555555555555555
    return x


# ── Upstream-compatible Z2SFC / XZ2SFC math ────────────────────────────────────
# Mirrors org.locationtech.geomesa.curve.{Z2SFC, XZ2SFC} from geomesa-z3:5.4.0.
# Z2SFC default precision = 31 bits/axis → 62-bit non-negative Long.
# XZ2SFC default g = 12 → sequence-code Long in roughly [0, 22M].

_Z2_PRECISION = 31
_Z2_BINS = 1 << _Z2_PRECISION
_Z2_MAX_IDX = _Z2_BINS - 1
_XZ2_G = 12


def _z2sfc_normalize(v: float, vmin: float, vmax: float) -> int:
    """NormalizedLat/NormalizedLon.normalize at 31-bit precision."""
    if v >= vmax:
        return _Z2_MAX_IDX
    if v < vmin:
        return 0
    import math
    return math.floor((v - vmin) * (_Z2_BINS / (vmax - vmin)))


def _z2sfc_index(lon: float, lat: float) -> int:
    """Z2SFC.index(lon, lat) at default precision (31 bits/axis)."""
    nx = _z2sfc_normalize(lon, -180.0, 180.0)
    ny = _z2sfc_normalize(lat,  -90.0,  90.0)
    return _spread_bits(nx) | (_spread_bits(ny) << 1)


def _xz2_sequence_code(x_norm: float, y_norm: float, length: int) -> int:
    """XZ2SFC.sequenceCode — pre-order quadtree index of the lower-left corner.
    `x_norm`, `y_norm` are in [0, 1]; `length` is the code length (=l1 or l1+1)."""
    xmin, ymin, xmax, ymax = 0.0, 0.0, 1.0, 1.0
    cs = 0
    g = _XZ2_G
    for i in range(length):
        xc = (xmin + xmax) / 2.0
        yc = (ymin + ymax) / 2.0
        # quadrant offset = 1 + k * (4^(g-i) - 1) / 3, where k is the quadrant index
        block = (pow(4, g - i) - 1) // 3
        if x_norm < xc and y_norm < yc:
            cs += 1
            xmax, ymax = xc, yc
        elif x_norm >= xc and y_norm < yc:
            cs += 1 + block
            xmin, ymax = xc, yc
        elif x_norm < xc and y_norm >= yc:
            cs += 1 + 2 * block
            xmax, ymin = xc, yc
        else:
            cs += 1 + 3 * block
            xmin, ymin = xc, yc
    return cs


def _xz2sfc_index(xmin: float, ymin: float, xmax: float, ymax: float) -> int:
    """XZ2SFC.index(envelope) at g=12; inputs clamped to WGS84 bounds
    (matches Java SfcBridge.xz2Index)."""
    import math
    xmin = min(180.0, max(-180.0, xmin))
    xmax = min(180.0, max(-180.0, xmax))
    ymin = min(90.0, max(-90.0, ymin))
    ymax = min(90.0, max(-90.0, ymax))
    # normalize to [0,1]
    nxmin = (xmin - (-180.0)) / 360.0
    nymin = (ymin - ( -90.0)) / 180.0
    nxmax = (xmax - (-180.0)) / 360.0
    nymax = (ymax - ( -90.0)) / 180.0

    max_dim = max(nxmax - nxmin, nymax - nymin)
    # Guard against the degenerate-point case where max_dim == 0 → log(0) = -inf.
    if max_dim <= 0.0:
        return _xz2_sequence_code(nxmin, nymin, _XZ2_G)

    l1 = math.floor(math.log(max_dim) / math.log(0.5))

    if l1 >= _XZ2_G:
        length = _XZ2_G
    else:
        w2 = pow(0.5, l1 + 1)

        def contains(lo: float, hi: float) -> bool:
            return hi <= (math.floor(lo / w2) * w2) + 2 * w2

        length = l1 + 1 if contains(nxmin, nxmax) and contains(nymin, nymax) else l1

    return _xz2_sequence_code(nxmin, nymin, length)


@dataclass(frozen=True)
class GeometryColumn:
    """Descriptor for a geometry column on an Iceberg table.

    name        — the VARBINARY column name (e.g. "geom", "center", "ellipse").
    index       — "z2" for point-centroid partitioning, "xz2" for envelope-cell
                  partitioning; None for bbox-only (no partition column).
    bbox        — emit a __<name>_bbox__ struct column when True (default).
    z2_bits     — total bit resolution for the z2/xz2 cell index. Ignored when
                  index is None.
    """
    name: str
    index: Optional[SpatialIndex] = None
    bbox: bool = True
    z2_bits: int = 12

    def __post_init__(self) -> None:
        if self.index is not None and self.index not in ("z2", "xz2"):
            raise ValueError(
                f"GeometryColumn.index must be 'z2', 'xz2', or None, got {self.index!r}")


def companion_names(g: GeometryColumn) -> dict[str, str]:
    """Return the companion column names for a GeometryColumn.

    Keys: 'bbox' (if g.bbox), 'partition' (if g.index is not None). Values
    follow the naming convention __<g.name>_<companion>__.
    """
    out: dict[str, str] = {}
    if g.bbox:
        out["bbox"] = f"__{g.name}_bbox__"
    if g.index is not None:
        out["partition"] = f"__{g.name}_{g.index}__"
    return out


def _bbox_struct_type(next_id: int) -> tuple[StructType, int]:
    """Return a (StructType, next_field_id_after) tuple for a bbox struct.

    Allocates four sequential sub-field IDs starting at next_id."""
    struct = StructType(
        NestedField(next_id,     "xmin", FloatType()),
        NestedField(next_id + 1, "ymin", FloatType()),
        NestedField(next_id + 2, "xmax", FloatType()),
        NestedField(next_id + 3, "ymax", FloatType()),
    )
    return struct, next_id + 4


VISIBILITIES_FIELD_NAME = "visibilities"

# Deterministic round-robin for demo/IT data: a notional U//FOUO clearance
# ladder, each marking component (U, FOUO) a separate auth token ANDed
# together. None = unmarked/public; a user holding {U,FOUO} sees every tier.
VIS_CYCLE = [None, "U", "U&FOUO"]


def visibilities_field(field_id: int) -> NestedField:
    """Optional per-row Accumulo-style visibility expression. The name matches
    the FSDS Iceberg-compatible parquet schema so registered GeoMesa tables and
    Python-ingested tables look identical to the datastore."""
    return NestedField(field_id, VISIBILITIES_FIELD_NAME, StringType(), required=False)


def cycle_visibility(i: int):
    """Visibility expression for synthetic row i (None = unrestricted)."""
    return VIS_CYCLE[i % len(VIS_CYCLE)]


def companion_fields(geoms: list[GeometryColumn], next_id: int) -> list[NestedField]:
    """Build the Iceberg NestedFields for the companion columns of each geom.

    Returns a flat list ordered as: for each geom in geoms, emit __<X>_bbox__
    (if g.bbox) then __<X>_{z2,xz2}__ (if g.index). next_id is the first
    top-level field-id to allocate; bbox struct sub-field IDs use a separate
    contiguous range starting at next_id + 10_000 to stay clear of other
    top-level allocations (callers are responsible for ensuring this gap is
    wide enough for their schema).

    Top-level field-ids are sequential starting at next_id. Sub-field IDs
    inside the bbox struct are allocated in a separate contiguous range
    starting at `next_id + 10_000`.
    """
    fields: list[NestedField] = []
    # Bbox sub-fields live in a separate range to avoid collision with top-level
    # field-ids. The +10_000 gap is sufficient for any realistic schema (top-level
    # ids in this codebase reach at most ~12); guard explicitly so a misconfigured
    # caller fails loudly rather than silently producing duplicate ids.
    max_top_level = next_id + 2 * len(geoms)  # bbox + partition per geom, at most
    if max_top_level >= next_id + 10_000:
        raise ValueError(
            f"companion_fields: {len(geoms)} geoms starting at next_id={next_id} "
            f"would exceed the 10_000-id sub-field gap; pass a smaller next_id or "
            f"widen the gap."
        )
    sub_id = next_id + 10_000
    for g in geoms:
        names = companion_names(g)
        if "bbox" in names:
            struct_type, sub_id = _bbox_struct_type(sub_id)
            fields.append(NestedField(next_id, names["bbox"], struct_type))
            next_id += 1
        if "partition" in names:
            fields.append(NestedField(next_id, names["partition"], StringType()))
            next_id += 1
    return fields


def geom_to_wkb(geom: BaseGeometry) -> bytes:
    return shapely_wkb.dumps(geom)


def geom_bbox(geom: BaseGeometry) -> tuple[float, float, float, float]:
    b = geom.bounds
    return b[0], b[1], b[2], b[3]


def geom_z2_raw(geom: BaseGeometry) -> int:
    """Z2SFC index of a Point geometry (31 bits/axis, non-negative 62-bit
    Long). Matches `Z2Transform.apply(wkb)` bit-for-bit. Point-only:
    centroid-indexing extents breaks query-side pruning — use XZ2."""
    if geom.is_empty:
        raise ValueError("Z2 index is undefined for empty geometries")
    if geom.geom_type != "Point":
        raise ValueError(
            f"Z2 index requires Point geometries (got {geom.geom_type}); "
            "use XZ2 for extended geometries"
        )
    return _z2sfc_index(geom.x, geom.y)


def geom_xz2_raw(geom: BaseGeometry) -> int:
    """XZ2SFC sequence code of the geometry's envelope at g=12 (non-negative
    Long in roughly [0, 22M]). Matches `XZ2Transform.apply(wkb)` bit-for-bit."""
    xmin, ymin, xmax, ymax = geom.bounds
    return _xz2sfc_index(xmin, ymin, xmax, ymax)


def _hex_encode(v: int) -> str:
    """16-char zero-padded lowercase unsigned hex. Low-level utility; used
    by both Z2 (after shift) and XZ2 (no shift). Mirrors
    `Z2Transform.hexEncode(long)`."""
    return format(v, "016x")


def geom_z2_raw_hex(geom: BaseGeometry) -> str:
    """Encoded Z2 column value for the geometry: Z2SFC index left-shifted by
    2 bits (to expose the hemisphere bits in the top hex char), then 16-char
    unsigned hex. Stored in the `__<X>_z2__` VARCHAR column; the partition
    spec's truncate(N) buckets it to the effective resolution declared on
    the GeometryColumn. Mirrors `Z2Transform.encodeColumn(long)`."""
    return _hex_encode(geom_z2_raw(geom) << 2)


def geom_xz2_raw_hex(geom: BaseGeometry) -> str:
    """Hex-encoded XZ2SFC value for the geometry (no shift — XZ2 sequence
    codes don't carry geographic info in their high bits in a way that a
    fixed bit-shift could exploit). At g=12 every stored value shares the
    leading 8 zero hex chars, so partition widths below 13 produce a single
    bucket for the whole table."""
    return _hex_encode(geom_xz2_raw(geom))


# Iceberg table properties applied at create_table time across all ingest scripts.
# Per-geom metrics-mode entries are added by callers using metrics_properties_for(geoms)
# (since the geom name is dynamic). The base properties below are geom-agnostic.
#
# IMPORTANT: PyIceberg 0.11.1 has a hardcoded downgrade that overrides any FULL/
# TRUNCATE metrics mode to COUNTS for any column whose name contains a dot
# (pyiceberg/io/pyarrow.py PyArrowStatisticsCollector.primitive). The
# enable_pyiceberg_nested_metrics() patch below removes that downgrade so the
# table properties below actually take effect. Properties stay set so the table
# is correct on a future PyIceberg release that drops the downgrade.
TABLE_PROPERTIES = {
    # Target file size for unpartitioned writes. PyIceberg's bin-packer splits the input
    # arrow table into chunks of approximately this size when writing. Set small (1 MiB)
    # so demo/benchmark datasets land in tens of files — large enough that each file's
    # bbox stats are tight, small enough that file-level pruning has visible effect.
    "write.target-file-size-bytes": "1048576",
}


def metrics_properties_for(geoms: list[GeometryColumn]) -> dict[str, str]:
    """Return per-geom write.metadata.metrics.column.__<X>_bbox__.{sub} = 'full'
    properties for each geom whose bbox is enabled. Under the truncate-
    partition scheme, the effective partition resolution N is encoded in the
    table's partition spec (TruncateTransform width), so no
    geomesa.partition.<X>.bits property is needed."""
    out: dict[str, str] = {}
    for g in geoms:
        if g.bbox:
            for sub in ("xmin", "ymin", "xmax", "ymax"):
                out[f"write.metadata.metrics.column.__{g.name}_bbox__.{sub}"] = "full"
    return out


_PYICEBERG_PATCH_BROKEN_MSG = (
    "PyIceberg internals changed: the nested-metrics patch in "
    "enable_pyiceberg_nested_metrics() no longer applies ({reason}). Without it, "
    "bbox sub-field stats silently downgrade to COUNTS and file-level bbox "
    "pruning stops working for newly ingested data. Re-port the patch against "
    "the installed PyIceberg (see pyiceberg/io/pyarrow.py "
    "PyArrowStatisticsCollector.primitive) and update the pin in requirements.txt."
)


def enable_pyiceberg_nested_metrics() -> None:
    """
    Remove PyIceberg's hardcoded nested-field metrics downgrade so user-configured
    write.metadata.metrics.column.<dotted.path> modes are honored. Idempotent.
    Tracks pyiceberg/io/pyarrow.py PyArrowStatisticsCollector.primitive (~line 2330)
    in PyIceberg 0.11.1; revisit on upgrade (requirements.txt pins <0.12).
    Raises RuntimeError if the patch target is missing or no longer effective —
    silent failure would invisibly disable bbox pruning.
    """
    from pyiceberg.io import pyarrow as _pyiceberg_pyarrow
    from pyiceberg.io.pyarrow import MetricsMode, MetricModeTypes
    from pyiceberg.table import TableProperties
    from pyiceberg.types import BinaryType, PrimitiveType, StringType

    if "primitive" not in vars(_pyiceberg_pyarrow.PyArrowStatisticsCollector):
        raise RuntimeError(_PYICEBERG_PATCH_BROKEN_MSG.format(
            reason="PyArrowStatisticsCollector no longer defines primitive()"))

    def primitive(self, primitive: PrimitiveType):
        column_name = self._schema.find_column_name(self._field_id)
        if column_name is None:
            return []
        metrics_mode = _pyiceberg_pyarrow.match_metrics_mode(self._default_mode)
        col_mode = self._properties.get(
            f"{TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX}.{column_name}"
        )
        if col_mode:
            metrics_mode = _pyiceberg_pyarrow.match_metrics_mode(col_mode)
        # For non-string/binary primitives, TRUNCATE has no meaning — promote to FULL.
        if (
            not isinstance(primitive, (StringType, BinaryType))
            and metrics_mode.type == MetricModeTypes.TRUNCATE
        ):
            metrics_mode = MetricsMode(MetricModeTypes.FULL)
        # NOTE: the upstream version downgrades nested fields to COUNTS here. We
        # intentionally do not, so column-mode overrides for nested paths apply.
        return [
            _pyiceberg_pyarrow.StatisticsCollector(
                field_id=self._field_id,
                iceberg_type=primitive,
                mode=metrics_mode,
                column_name=column_name,
            )
        ]

    _pyiceberg_pyarrow.PyArrowStatisticsCollector.primitive = primitive
    _verify_nested_metrics_patch(_pyiceberg_pyarrow)


def _verify_nested_metrics_patch(_pyiceberg_pyarrow) -> None:
    """Probe: a 'full' override on a nested float sub-field must come back FULL."""
    from pyiceberg.io.pyarrow import MetricModeTypes
    from pyiceberg.schema import Schema
    from pyiceberg.types import FloatType, NestedField, StructType

    schema = Schema(NestedField(1, "__probe_bbox__", StructType(
        NestedField(2, "xmin", FloatType(), required=False)), required=False))
    plan = _pyiceberg_pyarrow.compute_statistics_plan(schema, {
        "write.metadata.metrics.default": "counts",
        "write.metadata.metrics.column.__probe_bbox__.xmin": "full",
    })
    collector = plan.get(2)
    if collector is None or collector.mode.type != MetricModeTypes.FULL:
        raise RuntimeError(_PYICEBERG_PATCH_BROKEN_MSG.format(
            reason="patched pipeline still downgrades nested sub-field stats"))


# ── Bucket-routing ingest ─────────────────────────────────────────────────────

@dataclass
class ParseStats:
    """Tracks rows skipped during streaming parse (malformed lines, invalid coords)."""
    skipped: int = 0


def optimize_tables(conn, table_names: list[str]) -> None:
    """Run Trino's iceberg `ALTER TABLE ... EXECUTE optimize` on each table.

    chunked_append writes one Parquet file per (chunk, partition) tuple — so a
    multi-chunk ingest into a many-partition table produces tens to thousands of
    tiny files. OPTIMIZE compacts those within-partition fragments down to the
    table's `write.target-file-size-bytes` target, undoing the chunked-write
    multiplication without changing the partition spec.

    Routed through the iceberg catalog directly (not spatial_iceberg) because
    EXECUTE optimize is an Iceberg-native connector procedure. Failures are
    logged but non-fatal — the ingest is the valuable part.
    """
    for name in table_names:
        sql = f"ALTER TABLE iceberg.spatial.{name} EXECUTE optimize"
        print(f"  Compacting iceberg.spatial.{name} ...", end=" ", flush=True)
        try:
            cur = conn.cursor()
            cur.execute(sql)
            cur.fetchall()
            print("done")
        except Exception as exc:
            print(f"failed (continuing): {exc}")


def chunked_append(
    rows_iter: Iterable[tuple],
    targets: list[tuple[Callable[[list], "pa.Table"], object]],
    chunk_size: int = 500_000,
) -> int:
    """Stream rows into one or more Iceberg tables in fixed-size chunks.

    Each entry in `targets` is (build_table_fn, iceberg_table). Per chunk, every
    build_fn is called with the same row list and appended to its corresponding
    table. This lets callers append the same parsed rows to one or more tables,
    parsing the input exactly once: every table sees the same row data, just
    shaped through (potentially) different Arrow schemas.

    With identity(__geom_z2__) partitioning, PyIceberg bin-packs each append into
    one Parquet file per partition value. Per-file bbox stats are tight by
    construction (every row in a file shares the same Z2 cell), so global Z2
    ordering pre-write is unnecessary. Memory is bounded by chunk_size.
    """
    chunk: list[tuple] = []
    written = 0
    for row in rows_iter:
        chunk.append(row)
        if len(chunk) >= chunk_size:
            for build_fn, tbl in targets:
                tbl.append(build_fn(chunk))
            written += len(chunk)
            chunk.clear()
            print(f"  Appended {written:,} rows ...", end="\r")
    if chunk:
        for build_fn, tbl in targets:
            tbl.append(build_fn(chunk))
        written += len(chunk)
    print(f"  Appended {written:,} rows{' ' * 20}")
    return written


# ── Partition spec helpers ────────────────────────────────────────────────────

def partition_spec_for_geoms(schema, geoms: list[GeometryColumn],
                              dtg_grain: str | None = "day") -> "PartitionSpec":
    """Build a PartitionSpec from a list of GeometryColumn descriptors.

    One truncate transform per geom whose index is not None, named
    `<X>_z2` or `<X>_xz2`. Plus an optional dtg temporal transform
    (`day`/`month`/`year`) when the schema has a dtg TimestamptzType field
    and dtg_grain is not None.

    Raises ValueError if a geom requests partitioning but the corresponding
    __<X>_{z2,xz2}__ column is missing from the schema.
    """
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.transforms import (
        DayTransform, MonthTransform, TruncateTransform, YearTransform,
    )
    from pyiceberg.types import TimestamptzType

    _DTG_GRAINS = {
        "day":   (DayTransform,   "dtg_day"),
        "month": (MonthTransform, "dtg_month"),
        "year":  (YearTransform,  "dtg_year"),
    }
    if dtg_grain is not None and dtg_grain not in _DTG_GRAINS:
        raise ValueError(
            f"dtg_grain must be one of {sorted(_DTG_GRAINS)} or None, got {dtg_grain!r}")

    fields = []
    next_field_id = 1000

    for g in geoms:
        if g.index is None:
            continue
        part_col = f"__{g.name}_{g.index}__"
        part_field = next(
            (f for f in schema.fields
             if f.name == part_col and isinstance(f.field_type, StringType)),
            None,
        )
        if part_field is None:
            raise ValueError(
                f"GeometryColumn(name={g.name!r}, index={g.index!r}) requires a "
                f"StringType {part_col!r} column in the schema (under the hex-encoded "
                f"truncate-string partition scheme)."
            )
        # Truncate-string width = number of hex chars to keep. n_chars = ceil(bits/4).
        n_chars = (g.z2_bits + 3) // 4   # integer ceil-divide
        fields.append(PartitionField(
            source_id=part_field.field_id,
            field_id=next_field_id,
            transform=TruncateTransform(width=n_chars),
            name=f"{g.name}_{g.index}",
        ))
        next_field_id += 1

    if dtg_grain is not None:
        dtg_field = next(
            (f for f in schema.fields
             if f.name == "dtg" and isinstance(f.field_type, TimestamptzType)),
            None,
        )
        if dtg_field is not None:
            transform_cls, name = _DTG_GRAINS[dtg_grain]
            fields.append(PartitionField(
                source_id=dtg_field.field_id,
                field_id=next_field_id,
                transform=transform_cls(),
                name=name,
            ))

    return PartitionSpec(*fields)


_BBOX_PA_STRUCT = pa.struct([
    pa.field("xmin", pa.float32()),
    pa.field("ymin", pa.float32()),
    pa.field("xmax", pa.float32()),
    pa.field("ymax", pa.float32()),
])


def with_companion_columns(arrow_table: pa.Table,
                            geoms: list[GeometryColumn]) -> pa.Table:
    """Append __<X>_bbox__ and __<X>_{z2,xz2}__ columns for each geom in `geoms`.

    For each GeometryColumn g in `geoms`:
      - if g.bbox    and __<g.name>_bbox__   is missing, appends a struct column
      - if g.index   and __<g.name>_<idx>__  is missing, appends an int64 column

    Pre-existing companion columns are preserved (the helper is idempotent
    per column, not per-table). The source geom column (g.name) must exist in
    arrow_table; KeyError is raised otherwise.
    """
    out = arrow_table
    for g in geoms:
        if g.name not in arrow_table.column_names:
            raise KeyError(
                f"GeometryColumn(name={g.name!r}) — source column not in arrow table; "
                f"available: {arrow_table.column_names}")
        wkbs = arrow_table.column(g.name).to_pylist()
        shapes = [shapely_wkb.loads(b) if b is not None else None for b in wkbs]

        bbox_col = f"__{g.name}_bbox__"
        if g.bbox and bbox_col not in out.column_names:
            bboxes = []
            for shape in shapes:
                if shape is None:
                    bboxes.append(None)
                else:
                    minx, miny, maxx, maxy = shape.bounds
                    bboxes.append({
                        "xmin": float(minx), "ymin": float(miny),
                        "xmax": float(maxx), "ymax": float(maxy),
                    })
            out = out.append_column(bbox_col, pa.array(bboxes, _BBOX_PA_STRUCT))

        if g.index is not None:
            part_col = f"__{g.name}_{g.index}__"
            if part_col not in out.column_names:
                if g.index == "z2":
                    values = [
                        geom_z2_raw_hex(s) if s is not None else None
                        for s in shapes
                    ]
                else:  # "xz2"
                    values = [
                        geom_xz2_raw_hex(s) if s is not None else None
                        for s in shapes
                    ]
                out = out.append_column(part_col, pa.array(values, pa.string()))
    return out


