package org.locationtech.geomesa.accumulo.spark

import java.util.{Map => JMap}

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SQLContext, SQLTypes, SparkSession}
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.spark.SparkSQLTestUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AccumuloSparkProviderTest extends Specification with TestWithDataStore with LazyLogging {

  override lazy val sftName: String = "chicago"
  override def spec: String = SparkSQLTestUtils.ChiSpec

  "sql data tests" should {
    sequential

    var spark: SparkSession = null
    var sc: SQLContext = null

    var df: DataFrame = null

    // before
    step {
      spark = SparkSQLTestUtils.createSparkSession()
      sc = spark.sqlContext
      SQLTypes.init(sc)
      SparkSQLTestUtils.ingestChicago(ds)

      val params = dsParams.filterNot { case (k, _) => k == "connector" } ++ Map("useMock" -> true)
      df = spark.read
        .format("geomesa")
        .option("instanceId", mockInstanceId)
        .option("user", mockUser)
        .option("password", mockPassword)
        .options(params.map { case (k, v) => k -> v.toString })
        .option("geomesa.feature", "chicago")
        .load()

      logger.info(df.schema.treeString)
      df.createOrReplaceTempView("chicago")
    }

    "select by secondary indexed attribute" >> {
      val cases = df.select("case_number").where("case_number = 1").collect().map(_.getInt(0))
      cases.length mustEqual 1
    }

    "complex st_buffer" >> {
      val buf = sc.sql("select st_asText(st_bufferPoint(geom,10)) from chicago where case_number = 1").collect().head.getString(0)
      sc.sql(
        s"""
          |select *
          |from chicago
          |where
          |  st_contains(st_geomFromWKT('$buf'), geom)
         """.stripMargin
      ).collect().length must beEqualTo(1)
    }

  }

}

