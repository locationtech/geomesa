from py4j.java_gateway import java_import
from pyspark import RDD, SparkContext


class GeoMesaSpark:
    def __init__(self, sc):
        self.sc = sc
        self.jvm = sc._gateway.jvm

        java_import(self.jvm, "org.apache.hadoop.conf.Configuration")
        java_import(self.jvm, "org.geotools.data.Query")
        java_import(self.jvm, "org.geotools.filter.text.ecql.ECQL")
        java_import(self.jvm, "org.locationtech.geomesa.spark.api.python.PythonGeoMesaSpark")
        java_import(self.jvm, "org.locationtech.geomesa.spark.api.python.PythonSpatialRDDProvider")
        java_import(self.jvm, "org.locationtech.geomesa.spark.api.python.PythonSpatialRDD")

    def apply(self, params):
        provider = self.jvm.PythonGeoMesaSpark.apply(params)
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
        jrdd = self.__jrdd(typename, ecql).asKeyValueDict()
        return self.__pyrdd(jrdd)

    def rdd_tuples(self, typename, ecql):
        jrdd = self.__jrdd(typename, ecql).asKeyValueTupleList()
        return self.__pyrdd(jrdd)

    def rdd_values(self, typename, ecql):
        jrdd = self.__jrdd(typename, ecql).asValueList()
        return self.__pyrdd(jrdd)

    def __jrdd(self, typename, ecql):
        filter = self.jvm.ECQL.toFilter(ecql)
        query = self.jvm.Query(typename, filter)
        return self.provider.rdd(self.jvm.Configuration(), self.sc._jsc, self.params, query)

    def __pyrdd(self, jrdd):
        return RDD(jrdd, self.sc)
