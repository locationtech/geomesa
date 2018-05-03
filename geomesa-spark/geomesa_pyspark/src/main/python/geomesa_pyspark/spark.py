from py4j.java_gateway import java_import
from pyspark import RDD, SparkContext
from pyspark.sql.types import UserDefinedType, StructField, BinaryType
from pyspark.sql import Row


class GeoMesaSpark:
    def __init__(self, sc):
        self.sc = sc
        self.jvm = sc._gateway.jvm

        java_import(self.jvm, "org.apache.hadoop.conf.Configuration")
        java_import(self.jvm, "org.geotools.data.Query")
        java_import(self.jvm, "org.geotools.filter.text.ecql.ECQL")
        java_import(self.jvm, "org.locationtech.geomesa.spark.api.java.JavaGeoMesaSpark")
        java_import(self.jvm, "org.locationtech.geomesa.spark.api.java.JavaSpatialRDDProvider")
        java_import(self.jvm, "org.locationtech.geomesa.spark.api.java.JavaSpatialRDD")

    def apply(self, params):
        provider = self.jvm.JavaGeoMesaSpark.apply(params)
        return SpatialRDDProvider(self.sc, params, provider)


class SpatialRDDProvider:
    def __init__(self, sc, params, provider):
        self.sc = sc
        self.jvm = sc._gateway.jvm
        self.params = params
        self.provider = provider

    def rdd_geojson(self, typename, ecql):
        jrdd = self.__jrdd(typename, ecql).asGeoJSONString()
        return self.__pyrdd(jrdd)

    def rdd_dict(self, typename, ecql):
        jrdd = self.__jrdd(typename, ecql).asPyKeyValueMap()
        return self.__pyrdd(jrdd)

    def rdd_tuples(self, typename, ecql):
        jrdd = self.__jrdd(typename, ecql).asPyKeyValueList()
        return self.__pyrdd(jrdd)

    def rdd_values(self, typename, ecql):
        jrdd = self.__jrdd(typename, ecql).asPyValueList()
        return self.__pyrdd(jrdd)

    def __jrdd(self, typename, ecql):
        filter = self.jvm.ECQL.toFilter(ecql)
        query = self.jvm.Query(typename, filter)
        return self.provider.rdd(self.jvm.Configuration(), self.sc._jsc, self.params, query)

    def __pyrdd(self, jrdd):
        return RDD(self.jvm.SerDe.javaToPython(jrdd), self.sc)


class GeometryUDT(UserDefinedType):
    jvm = None

    @classmethod
    def sqlType(self):
        return StructField("wkb", BinaryType(), False)

    @classmethod
    def module(cls):
        return 'geomesa_pyspark'

    @classmethod
    def scalaUDT(cls):
        return 'org.apache.spark.sql.jts.GeometryUDT'

    def serialize(self, obj):
        if obj is None:
            return None
        return Row(obj.toBytes)

    def deserialize(self, datum):
        if self.jvm is None:
            self.jvm = SparkContext._active_spark_context._gateway.jvm
            java_import(self.jvm, "org.locationtech.geomesa.spark.jts.util.JavaAbstractGeometryUDT")
        return self.jvm.JavaAbstractGeometryUDT.deserialize(datum[0])
