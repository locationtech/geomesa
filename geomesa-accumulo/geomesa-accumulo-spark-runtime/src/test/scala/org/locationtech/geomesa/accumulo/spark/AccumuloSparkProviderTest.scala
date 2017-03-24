/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.spark

import java.util.{Map => JMap}

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SQLContext, SQLTypes, SparkSession}
import org.geotools.data.{Query, Transaction}
import org.geotools.factory.CommonFactoryFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.spark.SparkSQLTestUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AccumuloSparkProviderTest extends Specification with TestWithDataStore with LazyLogging {

  override lazy val sftName: String = "chicago"
  override def spec: String = SparkSQLTestUtils.ChiSpec
  private val ff = CommonFactoryFinder.getFilterFactory2

  "sql data tests" should {
    sequential

    var spark: SparkSession = null
    var sc: SQLContext = null

    var df: DataFrame = null

    val params = dsParams.filterNot { case (k, _) => k == "connector" } ++ Map("useMock" -> true)

    // before
    step {
      spark = SparkSQLTestUtils.createSparkSession()
      sc = spark.sqlContext
      SQLTypes.init(sc)
      SparkSQLTestUtils.ingestChicago(ds)

      df = spark.read
        .format("geomesa")
        .option("instanceId", mockInstanceId)
        .option("user", mockUser)
        .option("password", mockPassword)
        .options(params.map { case (k, v) => k -> v.toString })
        .option("geomesa.feature", "chicago")
        .load()

      logger.debug(df.schema.treeString)
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

    "write data and properly index" >> {
      val subset = sc.sql("select case_number,geom,dtg from chicago")
      subset.write.format("geomesa")
        .option("instanceId", mockInstanceId)
        .option("user", mockUser)
        .option("password", mockPassword)
        .options(params.map { case (k, v) => k -> v.toString })
        .option("geomesa.feature", "chicago2")
        .save()

      val sft = ds.getSchema("chicago2")
      val enabledIndexes = sft.getUserData.get("geomesa.indices").asInstanceOf[String]
      enabledIndexes.indexOf("z3") must be greaterThan -1
    }

    "handle reuse __fid__ on write if available" >> {
      val subset = sc.sql("select __fid__,case_number,geom,dtg from chicago")
      subset.write.format("geomesa")
        .option("instanceId", mockInstanceId)
        .option("user", mockUser)
        .option("password", mockPassword)
        .options(params.map { case (k, v) => k -> v.toString })
        .option("geomesa.feature", "fidOnWrite")
        .save()

      import org.locationtech.geomesa.utils.geotools.Conversions._
      val filter = ff.equals(ff.property("case_number"), ff.literal(1))
      val queryOrig = new Query("chicago", filter)
      val origResults = ds.getFeatureReader(queryOrig, Transaction.AUTO_COMMIT).toIterator.toList

      val query = new Query("fidOnWrite", filter)
      val results = ds.getFeatureReader(query, Transaction.AUTO_COMMIT).toIterator.toList

      results.head.getID must be equalTo origResults.head.getID
    }
  }

}

