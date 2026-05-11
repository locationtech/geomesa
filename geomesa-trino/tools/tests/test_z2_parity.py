import json
from pathlib import Path

import pytest
from shapely import wkb as shapely_wkb

from common import geom_z2_raw, geom_xz2_raw

CORPUS_PATH = Path(__file__).parent / "data" / "z2_parity_corpus.json"
XZ2_CORPUS_PATH = Path(__file__).parent / "data" / "xz2_parity_corpus.json"


@pytest.mark.parametrize("entry", json.loads(CORPUS_PATH.read_text()))
def test_python_z2_raw_matches_java_corpus(entry):
    geom = shapely_wkb.loads(bytes.fromhex(entry["wkb_hex"]))
    py_z2 = geom_z2_raw(geom)
    assert py_z2 == entry["z2"], (
        f"Z2 canonical-reference parity mismatch for {entry['wkt']}: "
        f"py={py_z2}, java={entry['z2']}"
    )


@pytest.mark.parametrize("entry", json.loads(XZ2_CORPUS_PATH.read_text()))
def test_python_xz2_raw_matches_java_corpus(entry):
    geom = shapely_wkb.loads(bytes.fromhex(entry["wkb_hex"]))
    actual = geom_xz2_raw(geom)
    expected = entry["xz2"]
    assert actual == expected, (
        f"XZ2 canonical-reference parity mismatch for wkt={entry['wkt']!r}: "
        f"Python={actual}, Java={expected}"
    )
