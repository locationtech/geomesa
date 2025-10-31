/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.spark.accumulo

import org.apache.spark.sql.DataFrame
import org.geomesa.testcontainers.AccumuloContainer
import org.geotools.api.data.{DataStoreFinder, Query, Transaction}
import org.locationtech.geomesa.spark.{SparkSQLTestUtils, TestWithSpark}
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.io.CloseWithLogging

class AccumuloSparkProviderTest extends TestWithSpark {

  import org.locationtech.geomesa.filter.ff

  import scala.collection.JavaConverters._

  private val accumulo =
    new AccumuloContainer()
      .withGeoMesaDistributedRuntime()
      .withNetwork(network)

  // these params will work in the spark executor, but not locally outside the docker network
  lazy val sparkDsParams = {
    val host = accumulo.execInContainer("hostname", "-s").getStdout.trim
    logger.debug("Using host: {}", host)
    Map(
      "accumulo.instance.name" -> accumulo.getInstanceName,
      "accumulo.zookeepers"    -> accumulo.getZookeepers.replace(accumulo.getHost, host),
      "accumulo.user"          -> accumulo.getUsername,
      "accumulo.password"      -> accumulo.getPassword,
      "accumulo.catalog"       -> "AccumuloSparkProviderTest"
    )
  }

  lazy val ds = {
    val params = sparkDsParams + ("accumulo.zookeepers" -> accumulo.getZookeepers)
    DataStoreFinder.getDataStore(params.asJava)
  }

  var df: DataFrame = _

  override def beforeAll(): Unit = {
    accumulo.start()

    SparkSQLTestUtils.ingestChicago(ds)

    // note: the host reach-back networking required for spark seems to mess up the accumulo networking unless accumulo starts first
    super.beforeAll()

    df = spark.read
      .format("geomesa")
      .options(sparkDsParams)
      .option("geomesa.feature", "chicago")
      .load()

    logger.debug(df.schema.treeString)
    df.createOrReplaceTempView("chicago")
  }

  override def afterAll(): Unit = {
    super.afterAll()
    CloseWithLogging(ds, accumulo)
  }

  "sql data tests" should {

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
        .options(sparkDsParams)
        .option("geomesa.feature", "chicago2")
        .save()

      val sft = ds.getSchema("chicago2")
      val enabledIndexes = sft.getUserData.get("geomesa.indices").asInstanceOf[String]
      enabledIndexes.indexOf("z3") must be greaterThan -1
    }

    "handle reuse __fid__ on write if available" >> {
      val subset = sc.sql("select __fid__,case_number,geom,dtg from chicago")
      subset.write.format("geomesa")
        .options(sparkDsParams)
        .option("geomesa.feature", "fidOnWrite")
        .save()
      val filter = ff.equals(ff.property("case_number"), ff.literal(1))
      val queryOrig = new Query("chicago", filter)
      val origResults = SelfClosingIterator(ds.getFeatureReader(queryOrig, Transaction.AUTO_COMMIT)).toList

      val query = new Query("fidOnWrite", filter)
      val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList

      results.head.getID must be equalTo origResults.head.getID
    }
  }
}
