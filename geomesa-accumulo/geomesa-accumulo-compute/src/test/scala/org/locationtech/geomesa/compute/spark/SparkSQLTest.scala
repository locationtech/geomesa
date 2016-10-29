package org.locationtech.geomesa.compute.spark

import org.apache.spark.sql.SparkSession
import org.locationtech.geomesa.accumulo.data.{ AccumuloDataStoreParams => GM }

object SparkSQLTest extends App {

  val spark = SparkSession.builder().master("local[*]").getOrCreate()

  val df = spark.read
    .format("geomesa")
    .option(GM.instanceIdParam.getName, "tcloud")
    .option(GM.zookeepersParam.getName, "tzoo1")
    .option(GM.userParam.getName, "root")
    .option(GM.passwordParam.getName, "secret")
    .option(GM.tableNameParam.getName, "geomesa126.chicago")
    .option("geomesa.feature", "chicago")
    .load()

  df.printSchema()

  df.createOrReplaceTempView("chicago")

  import spark.sqlContext.{ sql => $ }

  $("select arrest,case_number,latitude,longitude,geom from chicago limit 5").show()

  $("""
      |select  *
      |from    chicago
      |where   st_contains(geom, st_geomFromText('POLYGON((-88 41,-87 42,-87 42,-88 42,-88 41))'))
    """.stripMargin).show()


}
