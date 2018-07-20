from pyspark.sql.types import UserDefinedType, StructField, BinaryType, StructType
from shapely import wkb
from shapely.geometry import LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon
from shapely.geometry.base import BaseGeometry
from shapely.geometry.collection import GeometryCollection


class ShapelyGeometryUDT(UserDefinedType):

    @classmethod
    def sqlType(cls):
        return StructType([StructField("wkb", BinaryType(), True)])

    @classmethod
    def module(cls):
        return 'geomesa_pyspark.types'

    @classmethod
    def scalaUDT(cls):
        return 'org.apache.spark.sql.jts.' + cls.__name__

    def serialize(self, obj):
        return [_serialize_to_wkb(obj)]

    def deserialize(self, datum):
        return _deserialize_from_wkb(datum[0])


class PointUDT(ShapelyGeometryUDT):
    pass


class LineStringUDT(ShapelyGeometryUDT):
    pass


class PolygonUDT(ShapelyGeometryUDT):
    pass


class MultiPointUDT(ShapelyGeometryUDT):
    pass


class MultiLineStringUDT(ShapelyGeometryUDT):
    pass


class MultiPolygonUDT(ShapelyGeometryUDT):
    pass


class GeometryUDT(ShapelyGeometryUDT):
    pass


class GeometryCollectionUDT(ShapelyGeometryUDT):
    pass


def _serialize_to_wkb(data):
    if isinstance(data, BaseGeometry):
        return bytearray(data.wkb)  # bytearray(...) needed for Python 2 compat.
    return None


def _deserialize_from_wkb(data):
    if data is None:
        return None
    return wkb.loads(bytes(data))  # bytes(...) needed for Python 2 compat.


_deserialize_from_wkb.__safe_for_unpickling__ = True

# inject some PySpark constructs into Shapely's geometry types
Point.__UDT__ = PointUDT()
MultiPoint.__UDT__ = MultiPointUDT()
LineString.__UDT__ = LineStringUDT()
MultiLineString.__UDT__ = MultiLineStringUDT()
Polygon.__UDT__ = PolygonUDT()
MultiPolygon.__UDT__ = MultiPolygonUDT()
BaseGeometry.__UDT__ = GeometryUDT()
GeometryCollection.__UDT__ = GeometryCollectionUDT()

