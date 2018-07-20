from geomesa_pyspark.types import *
from shapely.wkt import loads
from unittest import TestCase, main


class PointUDTTest(TestCase):

    udt = Point.__UDT__

    def test_udt(self):
        self.assertIsInstance(self.udt, PointUDT)

    def test_udt_roundtrip(self):
        wkt = "POINT (30 10)"
        g = loads(wkt)
        self.assertEqual(self.udt.deserialize(self.udt.serialize(g)), g)


class LineStringUDTTest(TestCase):

    udt = LineString.__UDT__

    def test_udt(self):
        self.assertIsInstance(self.udt, LineStringUDT)

    def test_udt_roundtrip(self):
        wkt = "LINESTRING (30 10, 10 30, 40 40)"
        g = loads(wkt)
        self.assertEqual(self.udt.deserialize(self.udt.serialize(g)), g)


class PolygonUDTTest(TestCase):

    udt = Polygon.__UDT__

    def test_udt(self):
        self.assertIsInstance(self.udt, PolygonUDT)

    def test_roundtrip(self):
        wkt = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"
        g = loads(wkt)
        self.assertEqual(self.udt.deserialize(self.udt.serialize(g)), g)

    def test_roundtrip2(self):
        wkt = "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))"
        g = loads(wkt)
        self.assertEqual(self.udt.deserialize(self.udt.serialize(g)), g)


class MultiPointUDTTest(TestCase):

    udt = MultiPoint.__UDT__
    
    def test_udt(self):
        self.assertIsInstance(self.udt, MultiPointUDT)

    def test_udt_roundtrip(self):
        wkt = "MULTIPOINT ((10 40), (40 30), (20 20), (30 10))"
        g = loads(wkt)
        self.assertEqual(self.udt.deserialize(self.udt.serialize(g)), g)

    def test_udt_roundtrip2(self):
        wkt = "MULTIPOINT (10 40, 40 30, 20 20, 30 10)"
        g = loads(wkt)
        self.assertEqual(self.udt.deserialize(self.udt.serialize(g)), g)


class MultiLineStringUDTTest(TestCase):

    udt = MultiLineString.__UDT__

    def test_udt(self):
        self.assertIsInstance(self.udt, MultiLineStringUDT)

    def test_udt_roundtrip(self):
        wkt = "MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))"
        g = loads(wkt)
        self.assertEqual(self.udt.deserialize(self.udt.serialize(g)), g)


class MultiPolygonUDTTest(TestCase):
    
    udt = MultiPolygon.__UDT__

    def test_udt(self):
        self.assertIsInstance(self.udt, MultiPolygonUDT)

    def test_udt_roundtrip(self):
        wkt = "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))"
        g = loads(wkt)
        self.assertEqual(self.udt.deserialize(self.udt.serialize(g)), g)

    def test_udt_roundtrip2(self):
        wkt = """MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)),
         ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),
          (30 20, 20 15, 20 25, 30 20)))"""
        g = loads(wkt)
        self.assertEqual(self.udt.deserialize(self.udt.serialize(g)), g)


class MultiGeometryCollectionUDTTest(TestCase):

    udt = GeometryCollection.__UDT__

    def test_udt(self):
        self.assertIsInstance(self.udt, GeometryCollectionUDT)

    def test_udt_roundtrip(self):
        wkt = "GEOMETRYCOLLECTION(POINT(4 6),LINESTRING(4 6,7 10))"
        g = loads(wkt)
        self.assertEqual(self.udt.deserialize(self.udt.serialize(g)), g)


class GeometryUDTTest(TestCase):

    udt = BaseGeometry.__UDT__
    
    def test_udt(self):
        self.assertIsInstance(self.udt, GeometryUDT)

    def test_udt_rouundtrip(self):
        wkt = "POINT (0 0)"
        g = loads(wkt)
        self.assertEqual(self.udt.deserialize(self.udt.serialize(g)), g)


if __name__ == '__main__':
    main()