from common import GeometryColumn, companion_names, companion_fields
from pyiceberg.types import NestedField, StructType, LongType, FloatType
import pytest


def test_geometry_column_defaults():
    g = GeometryColumn(name="geom")
    assert g.name == "geom"
    assert g.index is None
    assert g.bbox is True
    assert g.z2_bits == 12


def test_companion_names_for_z2_geom():
    names = companion_names(GeometryColumn(name="center", index="z2"))
    assert names == {"bbox": "__center_bbox__", "partition": "__center_z2__"}


def test_companion_names_for_xz2_geom():
    names = companion_names(GeometryColumn(name="ellipse", index="xz2"))
    assert names == {"bbox": "__ellipse_bbox__", "partition": "__ellipse_xz2__"}


def test_companion_names_for_bbox_only_geom():
    names = companion_names(GeometryColumn(name="region", index=None))
    assert names == {"bbox": "__region_bbox__"}


def test_companion_names_bbox_false_omits_bbox():
    names = companion_names(GeometryColumn(name="point", index="z2", bbox=False))
    assert names == {"partition": "__point_z2__"}


def test_invalid_index_raises():
    with pytest.raises(ValueError, match="index"):
        GeometryColumn(name="foo", index="quadtree")  # type: ignore[arg-type]


def test_companion_fields_for_z2_geom():
    g = GeometryColumn(name="geom", index="z2")
    fields = companion_fields([g], next_id=10)
    names = [f.name for f in fields]
    assert names == ["__geom_bbox__", "__geom_z2__"]
    # __geom_bbox__ is StructType with the four float sub-fields
    assert isinstance(fields[0].field_type, StructType)
    sub_names = [sub.name for sub in fields[0].field_type.fields]
    assert sub_names == ["xmin", "ymin", "xmax", "ymax"]
    # __geom_z2__ is StringType under the hex-encoded partition scheme.
    from pyiceberg.types import StringType
    assert isinstance(fields[1].field_type, StringType)


def test_companion_fields_for_multi_geom():
    geoms = [
        GeometryColumn(name="center", index="z2"),
        GeometryColumn(name="ellipse", index="xz2"),
    ]
    fields = companion_fields(geoms, next_id=100)
    names = [f.name for f in fields]
    assert names == [
        "__center_bbox__",  "__center_z2__",
        "__ellipse_bbox__", "__ellipse_xz2__",
    ]
    # top-level field_ids must be sequential starting from next_id
    ids = [f.field_id for f in fields]
    assert ids == [100, 101, 102, 103]  # 4 top-level fields; bbox sub-fields are separate


from shapely.geometry import Point as _Point
from common import geom_z2_raw as _geom_z2_raw, geom_xz2_raw as _geom_xz2_raw, geom_z2_raw_hex, geom_xz2_raw_hex


def test_geom_z2_raw_returns_max_62bit_value_for_north_pole_corner():
    # Z2SFC at 31-bit precision: (180, 90) lands at the (maxIndex, maxIndex)
    # cell, producing the 62-bit max value. Bit 63 is NEVER set — top 2 bits
    # of any Z2SFC output are zero by construction (sign bit reserved per the
    # MaxMask = 0x7fffffff convention upstream).
    z = _geom_z2_raw(_Point(180, 90))
    assert z == (1 << 62) - 1
    assert (z & (1 << 63)) == 0


def test_geom_z2_raw_distinct_for_distinct_points():
    a = _geom_z2_raw(_Point(-77.0, 38.9))
    b = _geom_z2_raw(_Point(116.0, 39.9))
    assert a != b


def test_geom_xz2_raw_differs_from_geom_z2_raw_for_point():
    # XZ2SFC.index at g=12 returns a sequence-code Long (small int ≤ ~22M),
    # while Z2SFC.index returns a 62-bit non-negative Long — different
    # encodings, different magnitudes.
    p = _Point(-77.0, 38.9)
    z2 = _geom_z2_raw(p)
    xz2 = _geom_xz2_raw(p)
    assert xz2 < (1 << 25)  # g=12 sequence codes fit in ~25 bits
    assert z2 > (1 << 60)   # this DC longitude lands in the upper half


def test_geom_z2_raw_hex_is_16_char_lowercase():
    h = geom_z2_raw_hex(_Point(-77.0, 38.9))
    assert len(h) == 16
    assert h == h.lower()
    assert all(c in "0123456789abcdef" for c in h)


def test_geom_z2_raw_hex_round_trips_with_shift():
    # The hex column value is `Z2SFC.index << 2` so the top hex char uses the
    # full 4-bit range; int(hex, 16) decodes back to raw << 2.
    p = _Point(-77.0, 38.9)
    raw = _geom_z2_raw(p)
    assert int(geom_z2_raw_hex(p), 16) == (raw << 2)


def test_geom_z2_raw_hex_extremes_use_full_hex_range():
    # SW corner (Z2=0) → "0000...0000". NE corner (Z2=2^62-1, after shift
    # = 2^64-4) → "fffffffffffffffc".
    assert geom_z2_raw_hex(_Point(-180.0, -90.0)) == "0000000000000000"
    assert geom_z2_raw_hex(_Point(180.0, 90.0)) == "fffffffffffffffc"


def test_geom_xz2_raw_hex_shares_prefix_at_g12():
    # XZ2 sequence codes at g=12 stay below 2^25, so every 16-char unsigned
    # hex value shares the leading 8 zero hex chars.
    h_dc    = geom_xz2_raw_hex(_Point(-77.0, 38.9))
    h_tokyo = geom_xz2_raw_hex(_Point(139.7, 35.7))
    assert h_dc[:8] == "00000000"
    assert h_tokyo[:8] == "00000000"
    assert h_dc != h_tokyo  # distinct sequence codes within the prefix


from pyiceberg.types import StringType


def test_companion_fields_z2_uses_string_type():
    g = GeometryColumn(name="geom", index="z2")
    fields = companion_fields([g], next_id=10)
    z2_field = next(f for f in fields if f.name == "__geom_z2__")
    assert isinstance(z2_field.field_type, StringType)


def test_companion_fields_xz2_uses_string_type():
    g = GeometryColumn(name="region", index="xz2")
    fields = companion_fields([g], next_id=10)
    xz2_field = next(f for f in fields if f.name == "__region_xz2__")
    assert isinstance(xz2_field.field_type, StringType)
