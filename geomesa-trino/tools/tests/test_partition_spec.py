import pyarrow as pa
import pytest
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BinaryType, LongType, NestedField, StringType, TimestamptzType,
)
from shapely.geometry import Point

from shapely.geometry import Polygon

from common import (
    GeometryColumn,
    companion_fields,
    geom_to_wkb,
    geom_z2_raw,
    geom_xz2_raw,
    geom_z2_raw_hex,
    geom_xz2_raw_hex,
    metrics_properties_for,
    partition_spec_for_geoms,
    with_companion_columns,
)


# ── Tests for partition_spec_for_geoms ───────────────────────────────────────

def _schema_with_geoms(geoms: list[GeometryColumn]) -> Schema:
    fields = [
        NestedField(1, "__fid__", StringType(), required=True),
        NestedField(2, "dtg", TimestamptzType()),
    ]
    next_id = 3
    for g in geoms:
        fields.append(NestedField(next_id, g.name, BinaryType()))
        next_id += 1
    fields.extend(companion_fields(geoms, next_id=next_id))
    return Schema(*fields)


def test_partition_spec_for_geoms_single_z2():
    geoms = [GeometryColumn(name="geom", index="z2")]
    spec = partition_spec_for_geoms(_schema_with_geoms(geoms), geoms, dtg_grain=None)
    assert len(spec.fields) == 1
    assert spec.fields[0].name == "geom_z2"


def test_partition_spec_for_geoms_single_xz2():
    geoms = [GeometryColumn(name="region", index="xz2")]
    spec = partition_spec_for_geoms(_schema_with_geoms(geoms), geoms, dtg_grain=None)
    assert len(spec.fields) == 1
    assert spec.fields[0].name == "region_xz2"


def test_partition_spec_for_geoms_multi_independent():
    geoms = [
        GeometryColumn(name="center",  index="z2"),
        GeometryColumn(name="ellipse", index="xz2"),
    ]
    spec = partition_spec_for_geoms(_schema_with_geoms(geoms), geoms, dtg_grain=None)
    field_names = [f.name for f in spec.fields]
    assert field_names == ["center_z2", "ellipse_xz2"]


def test_partition_spec_for_geoms_skips_geoms_without_index():
    geoms = [
        GeometryColumn(name="primary",   index="z2"),
        GeometryColumn(name="secondary", index=None),  # bbox-only, no partition
    ]
    spec = partition_spec_for_geoms(_schema_with_geoms(geoms), geoms, dtg_grain=None)
    field_names = [f.name for f in spec.fields]
    assert field_names == ["primary_z2"]


def test_partition_spec_for_geoms_with_dtg_grain():
    geoms = [GeometryColumn(name="geom", index="z2")]
    spec = partition_spec_for_geoms(_schema_with_geoms(geoms), geoms, dtg_grain="month")
    field_names = [f.name for f in spec.fields]
    assert field_names == ["geom_z2", "dtg_month"]


def test_partition_spec_for_geoms_missing_partition_column_raises():
    """Schema is missing __center_z2__ but geom descriptor requests index=z2."""
    schema = Schema(
        NestedField(1, "__fid__", StringType(), required=True),
        NestedField(2, "center", BinaryType()),
        # NO __center_z2__ field
    )
    import pytest
    with pytest.raises(ValueError, match="__center_z2__"):
        partition_spec_for_geoms(schema, [GeometryColumn(name="center", index="z2")],
                                  dtg_grain=None)


# ── Tests for with_companion_columns ─────────────────────────────────────────

def _arrow_from_geoms(geoms_map: dict[str, list]) -> pa.Table:
    cols: dict[str, pa.Array] = {
        "__fid__": pa.array([f"row-{i}" for i in range(len(next(iter(geoms_map.values()))))],
                            pa.string()),
    }
    for col_name, geom_list in geoms_map.items():
        cols[col_name] = pa.array([geom_to_wkb(g) for g in geom_list], pa.large_binary())
    return pa.table(cols)


def test_with_companion_columns_single_geom_appends_bbox_and_z2():
    geoms = [GeometryColumn(name="geom", index="z2", z2_bits=8)]
    arrow = _arrow_from_geoms({"geom": [Point(-77.5, 38.9), Point(0, 0)]})
    out = with_companion_columns(arrow, geoms)
    assert "__geom_bbox__" in out.column_names
    assert "__geom_z2__" in out.column_names
    # bbox is a struct, z2 is string
    assert out.schema.field("__geom_z2__").type == pa.string()


def test_with_companion_columns_multi_geom_independent_companions():
    geoms = [
        GeometryColumn(name="center",  index="z2",  z2_bits=8),
        GeometryColumn(name="ellipse", index="xz2", z2_bits=8),
    ]
    poly = Polygon([(-1, -1), (1, -1), (1, 1), (-1, 1)])
    arrow = _arrow_from_geoms({"center": [Point(0, 0)], "ellipse": [poly]})
    out = with_companion_columns(arrow, geoms)
    assert "__center_bbox__"   in out.column_names
    assert "__center_z2__"     in out.column_names
    assert "__ellipse_bbox__"  in out.column_names
    assert "__ellipse_xz2__"   in out.column_names


def test_with_companion_columns_idempotent_when_columns_exist():
    geoms = [GeometryColumn(name="geom", index="z2", z2_bits=8)]
    arrow = pa.table({
        "__fid__":      pa.array(["r0"],  pa.string()),
        "geom":         pa.array([geom_to_wkb(Point(0, 0))], pa.large_binary()),
        "__geom_z2__":  pa.array([42],    pa.int64()),
    })
    out = with_companion_columns(arrow, geoms)
    # __geom_z2__ existed, value preserved; __geom_bbox__ is added.
    assert out.column("__geom_z2__").to_pylist() == [42]
    assert "__geom_bbox__" in out.column_names


def test_with_companion_columns_skips_partition_when_bbox_only():
    geoms = [GeometryColumn(name="region", index=None, bbox=True)]
    arrow = _arrow_from_geoms({"region": [Polygon([(0,0),(1,0),(1,1),(0,1)])]})
    out = with_companion_columns(arrow, geoms)
    assert "__region_bbox__" in out.column_names
    assert "__region_z2__"   not in out.column_names
    assert "__region_xz2__"  not in out.column_names


# ── Tests for metrics_properties_for ─────────────────────────────────────────

def test_metrics_properties_for_emits_per_geom_bbox_metrics_only():
    geoms = [
        GeometryColumn(name="center",  index="z2",  z2_bits=8),
        GeometryColumn(name="ellipse", index="xz2", z2_bits=10),
    ]
    props = metrics_properties_for(geoms)
    # bbox metrics still emitted per geom
    assert props["write.metadata.metrics.column.__center_bbox__.xmin"]  == "full"
    assert props["write.metadata.metrics.column.__center_bbox__.ymax"]  == "full"
    assert props["write.metadata.metrics.column.__ellipse_bbox__.xmin"] == "full"
    # geomesa.partition.<X>.bits is NO LONGER emitted — the partition spec
    # encodes the effective resolution via the TruncateTransform width.
    assert "geomesa.partition.center.bits"  not in props
    assert "geomesa.partition.ellipse.bits" not in props


def test_metrics_properties_for_skips_bbox_when_disabled():
    geoms = [GeometryColumn(name="point", index="z2", bbox=False)]
    props = metrics_properties_for(geoms)
    assert "write.metadata.metrics.column.__point_bbox__.xmin" not in props
    # No bits property, regardless of index presence.
    assert "geomesa.partition.point.bits" not in props


# ── Tests for partition_spec_for_geoms with TruncateTransform ──────────────────

from pyiceberg.transforms import TruncateTransform


def test_partition_spec_for_geoms_z2_uses_truncate_string_transform():
    geoms = [GeometryColumn(name="geom", index="z2", z2_bits=20)]
    spec = partition_spec_for_geoms(_schema_with_geoms(geoms), geoms, dtg_grain=None)
    assert len(spec.fields) == 1
    f = spec.fields[0]
    assert f.name == "geom_z2"
    assert isinstance(f.transform, TruncateTransform)
    # n_chars = ceil(20 / 4) = 5
    assert f.transform.width == 5


def test_partition_spec_for_geoms_xz2_uses_truncate_string_transform():
    geoms = [GeometryColumn(name="region", index="xz2", z2_bits=8)]
    spec = partition_spec_for_geoms(_schema_with_geoms(geoms), geoms, dtg_grain=None)
    f = spec.fields[0]
    assert f.name == "region_xz2"
    assert isinstance(f.transform, TruncateTransform)
    # n_chars = ceil(8 / 4) = 2
    assert f.transform.width == 2


def test_partition_spec_for_geoms_n_chars_max_64():
    geoms = [GeometryColumn(name="geom", index="z2", z2_bits=64)]
    spec = partition_spec_for_geoms(_schema_with_geoms(geoms), geoms, dtg_grain=None)
    f = spec.fields[0]
    # n_chars = ceil(64 / 4) = 16 (full 16-char hex column unchanged)
    assert f.transform.width == 16


def test_partition_spec_for_geoms_n_chars_rounds_up_for_non_multiples_of_4():
    # geolife uses z2_bits=18 today; under hex encoding, rounds up to 5
    # chars (effective 20-bit resolution).
    geoms = [GeometryColumn(name="geom", index="z2", z2_bits=18)]
    spec = partition_spec_for_geoms(_schema_with_geoms(geoms), geoms, dtg_grain=None)
    f = spec.fields[0]
    # ceil(18 / 4) = 5
    assert f.transform.width == 5


# ── Tests for with_companion_columns writing canonical-reference values ───────

def test_with_companion_columns_z2_value_matches_geom_z2_raw_hex():
    geoms = [GeometryColumn(name="geom", index="z2", z2_bits=20)]
    arrow = _arrow_from_geoms({"geom": [Point(-77.0, 38.9)]})
    out = with_companion_columns(arrow, geoms)
    stored = out.column("__geom_z2__").to_pylist()[0]
    expected = geom_z2_raw_hex(Point(-77.0, 38.9))
    assert stored == expected
    # Column type is string, not int64.
    assert out.schema.field("__geom_z2__").type == pa.string()


def test_with_companion_columns_xz2_value_matches_geom_xz2_raw_hex():
    from shapely.geometry import Polygon as _Polygon
    geoms = [GeometryColumn(name="region", index="xz2", z2_bits=8)]
    poly = _Polygon([(-1, -1), (1, -1), (1, 1), (-1, 1)])
    arrow = _arrow_from_geoms({"region": [poly]})
    out = with_companion_columns(arrow, geoms)
    stored = out.column("__region_xz2__").to_pylist()[0]
    expected = geom_xz2_raw_hex(poly)
    assert stored == expected
    assert out.schema.field("__region_xz2__").type == pa.string()
