import pytest
from shapely.geometry import Point, Polygon
from common import (
    geom_to_wkb,
    geom_bbox,
    geom_z2_raw,
    geom_xz2_raw,
    geom_z2_raw_hex,
    geom_xz2_raw_hex,
)


def test_geom_to_wkb_produces_bytes():
    wkb = geom_to_wkb(Point(-77.0, 38.9))
    assert isinstance(wkb, bytes)
    assert len(wkb) > 0


def test_geom_to_wkb_roundtrip():
    from shapely import wkb
    pt = Point(-77.0, 38.9)
    assert wkb.loads(geom_to_wkb(pt)).equals(pt)


def test_geom_bbox_point():
    minx, miny, maxx, maxy = geom_bbox(Point(1.0, 2.0))
    assert minx == maxx == 1.0
    assert miny == maxy == 2.0


def test_geom_bbox_polygon():
    poly = Polygon([(0, 0), (2, 0), (2, 3), (0, 3)])
    minx, miny, maxx, maxy = geom_bbox(poly)
    assert minx == 0.0 and maxx == 2.0
    assert miny == 0.0 and maxy == 3.0


def test_geom_z2_raw_is_nonnegative_62bit():
    z = geom_z2_raw(Point(-77.0, 38.9))
    assert isinstance(z, int)
    assert 0 <= z < (1 << 62)


def test_geom_z2_raw_deterministic():
    assert geom_z2_raw(Point(-77.0, 38.9)) == geom_z2_raw(Point(-77.0, 38.9))


def test_geom_z2_raw_distant_points_differ():
    assert geom_z2_raw(Point(-77.0, 38.9)) != geom_z2_raw(Point(2.3, 48.8))


def test_geom_z2_raw_extremes():
    assert geom_z2_raw(Point(-180.0, -90.0)) == 0
    assert geom_z2_raw(Point(180.0, 90.0)) == (1 << 62) - 1


def test_geom_z2_raw_rejects_extended_geometries():
    # point-only: centroid-indexing extents breaks query-side pruning
    tri = Polygon([(0, 0), (2, 0), (0, 2)])
    with pytest.raises(ValueError, match="[Pp]oint"):
        geom_z2_raw(tri)


def test_geom_z2_raw_rejects_empty_geometry():
    with pytest.raises(ValueError, match="empty"):
        geom_z2_raw(Point())


def test_geom_xz2_raw_nonnegative_for_polygons():
    poly = Polygon([(-77.0, 38.9), (-76.0, 38.9), (-76.0, 39.9), (-77.0, 39.9)])
    x = geom_xz2_raw(poly)
    assert isinstance(x, int)
    assert x >= 0


def test_geom_xz2_raw_world_spanning_polygon_is_low_sequence_code():
    huge = Polygon([(-179.0, -89.0), (179.0, -89.0), (179.0, 89.0), (-179.0, 89.0)])
    # A polygon nearly spanning the world lands at level 1 — sequence code in [1, 4].
    assert 1 <= geom_xz2_raw(huge) <= 4


def test_geom_z2_raw_hex_is_16_char_lowercase():
    h = geom_z2_raw_hex(Point(-77.0, 38.9))
    assert len(h) == 16
    assert h == h.lower()
    assert not h.startswith("-")


def test_geom_z2_raw_hex_shifted_extremes():
    # Z2 values are non-negative 62-bit Longs left-shifted by 2 in the hex
    # encoding, so the full 16-char hex range is used: SW corner → "0000…",
    # NE corner near max → starts with "f".
    assert geom_z2_raw_hex(Point(-180.0, -90.0)) == "0000000000000000"
    assert geom_z2_raw_hex(Point(180.0, 90.0))[0] == "f"


def test_geom_xz2_raw_hex_is_16_char_lowercase():
    h = geom_xz2_raw_hex(Polygon([(-1, -1), (1, -1), (1, 1), (-1, 1)]))
    assert len(h) == 16
    assert h == h.lower()


def test_geom_xz2_raw_hex_shares_8char_prefix():
    # XZ2 sequence codes at g=12 are small ints (≤ ~22M ≈ 2^25), so every
    # 16-char unsigned-hex value shares the leading 8 zero hex chars.
    h1 = geom_xz2_raw_hex(Polygon([(-77, 38), (-76, 38), (-76, 39), (-77, 39)]))
    h2 = geom_xz2_raw_hex(Polygon([(110, 30), (120, 30), (120, 40), (110, 40)]))
    assert h1[:8] == "00000000"
    assert h2[:8] == "00000000"


def test_z2_clamps_out_of_bounds_coordinates_to_wgs84():
    # both SFC ports clamp (matches Java SfcBridge)
    assert geom_z2_raw(Point(180.0000001, 39.0)) == geom_z2_raw(Point(180.0, 39.0))
    assert geom_z2_raw(Point(-180.0000001, 39.0)) == geom_z2_raw(Point(-180.0, 39.0))
    assert geom_z2_raw(Point(116.0, 90.0000001)) == geom_z2_raw(Point(116.0, 90.0))


def test_xz2_clamps_out_of_bounds_envelope_to_wgs84():
    # Must match Java SfcBridge.xz2Index, which clamps before XZ2SFC(g).index.
    from shapely.geometry import box
    assert geom_xz2_raw(box(-180.0000001, 39.0, -179.0, 90.0000001)) == \
        geom_xz2_raw(box(-180.0, 39.0, -179.0, 90.0))
    assert geom_xz2_raw(box(179.5, 89.5, 180.0000001, 90.0000001)) == \
        geom_xz2_raw(box(179.5, 89.5, 180.0, 90.0))


def test_enable_pyiceberg_nested_metrics_fails_loudly_when_target_missing(monkeypatch):
    from pyiceberg.io import pyarrow as pyiceberg_pyarrow
    from common import enable_pyiceberg_nested_metrics

    monkeypatch.delattr(pyiceberg_pyarrow.PyArrowStatisticsCollector, "primitive")
    with pytest.raises(RuntimeError, match="PyIceberg"):
        enable_pyiceberg_nested_metrics()


def test_enable_pyiceberg_nested_metrics_yields_full_stats_for_nested_override():
    # upgrade tripwire: fails if a PyIceberg upgrade routes around the patch
    from pyiceberg.io.pyarrow import compute_statistics_plan, MetricModeTypes
    from pyiceberg.schema import Schema
    from pyiceberg.types import FloatType, NestedField, StructType
    from common import enable_pyiceberg_nested_metrics

    enable_pyiceberg_nested_metrics()
    schema = Schema(NestedField(1, "__probe_bbox__", StructType(
        NestedField(2, "xmin", FloatType(), required=False)), required=False))
    plan = compute_statistics_plan(schema, {
        "write.metadata.metrics.default": "counts",
        "write.metadata.metrics.column.__probe_bbox__.xmin": "full",
    })
    assert plan[2].mode.type == MetricModeTypes.FULL


def test_trino_host_port_defaults_and_env_override(monkeypatch):
    from common import trino_host_port

    monkeypatch.delenv("TRINO_HOST", raising=False)
    monkeypatch.delenv("TRINO_PORT", raising=False)
    assert trino_host_port() == ("localhost", 8080)

    monkeypatch.setenv("TRINO_HOST", "trino.example.com")
    monkeypatch.setenv("TRINO_PORT", "8443")
    assert trino_host_port() == ("trino.example.com", 8443)


def test_visibilities_field_matches_fsds_convention():
    from common import visibilities_field, VISIBILITIES_FIELD_NAME
    f = visibilities_field(7)
    assert f.field_id == 7
    assert f.name == VISIBILITIES_FIELD_NAME == "visibilities"
    assert not f.required


def test_cycle_visibility_is_deterministic():
    from common import cycle_visibility
    assert [cycle_visibility(i) for i in range(6)] == \
        [None, "U", "U&FOUO", None, "U", "U&FOUO"]
