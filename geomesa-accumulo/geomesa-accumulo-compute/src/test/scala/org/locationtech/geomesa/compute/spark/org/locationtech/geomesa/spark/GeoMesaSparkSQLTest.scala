package org.locationtech.geomesa.compute.spark.org.locationtech.geomesa.spark

import org.apache.spark.sql.SparkSession
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAfterExample

/**
  * Created by afox on 9/25/16.
  */
class GeoMesaSparkSQLTest extends Specification with BeforeAfterExample {

  var spark: SparkSession = null

  override protected def before: Any = {
    println("bbefore")
    spark = SparkSession.builder().master("local[*]").config("spark.ui.enabled", "false").getOrCreate()
    println("abefore")

  }

  override protected def after: Any = {
    spark.stop()
  }

  "must get result" >> {
    import org.locationtech.geomesa.spark.GeoMesaSparkSQL._

    val sft = SimpleFeatureTypes.createType("foo", "*geom:Point:srid=4326")

    val rdd = spark.sqlContext.simpleFeatureRelation(sft)

    val df = spark.baseRelationToDataFrame(rdd)

    df.createOrReplaceTempView("foo")


    spark.sql("DESCRIBE foo").show()

    "foo" must be equalTo "foo"
  }

}
