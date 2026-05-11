"""Both benchmarks must draw WKTs from benchmark_datasets.json's geometries
block — a hardcoded copy diverged once (AIS_LARGE), making the Java and Python
numbers silently incomparable."""
import json
import re
from pathlib import Path

import benchmark

CANONICAL = (
    Path(__file__).resolve().parents[2]
    / "geomesa-trino-benchmark" / "src" / "main" / "resources"
    / "benchmark_datasets.json"
)


def _geometries() -> dict:
    data = json.loads(CANONICAL.read_text())
    return {k: v for k, v in data["geometries"].items() if not k.startswith("_")}


def test_module_constants_match_canonical_geometries():
    g = _geometries()
    assert benchmark.NE_US_WKT == g["NE_US"]
    assert benchmark.SMALL_BOX_WKT == g["SMALL_BOX"]
    assert benchmark.DC_WKT == g["DC_PT"]
    assert benchmark.BEIJING_LARGE_WKT == g["BJ_LARGE"]
    assert benchmark.BEIJING_SMALL_WKT == g["BJ_TIGHT"]
    assert benchmark.TIANANMEN_WKT == g["TIANANMEN"]
    assert benchmark.GEOLIFE_LARGE_WKT == g["GL_LARGE"]
    assert benchmark.AIS_LARGE_WKT == g["AIS_LARGE"]


def test_every_dataset_config_wkt_is_canonical():
    """Every config WKT must exist verbatim in the canonical block."""
    canonical_values = set(_geometries().values())
    for table, cfg in benchmark.DATASET_CONFIGS.items():
        for key in ("large_wkt", "small_wkt", "dwithin_wkt"):
            wkt = cfg.get(key)
            if wkt is not None:
                assert wkt in canonical_values, (
                    f"{table}.{key} is not in the canonical geometries block: {wkt}"
                )


def test_ais_envelopes_overlap_zone17_data_extent():
    """AIS data is UTM Zone 17 (84W-78W); envelopes must overlap it or the
    benchmark measures empty space."""
    zone17_min_lon, zone17_max_lon = -84.0, -78.0
    cfg = benchmark.DATASET_CONFIGS["ais"]
    for key in ("large_wkt", "small_wkt", "dwithin_wkt"):
        wkt = cfg.get(key)
        if wkt is None:
            continue
        coords = [
            tuple(map(float, p.split()))
            for p in re.findall(r"-?\d+\.?\d*\s+-?\d+\.?\d*", wkt)
        ]
        lons = [c[0] for c in coords]
        assert min(lons) <= zone17_max_lon and max(lons) >= zone17_min_lon, (
            f"ais.{key} envelope {wkt} does not overlap the Zone 17 data extent"
        )
