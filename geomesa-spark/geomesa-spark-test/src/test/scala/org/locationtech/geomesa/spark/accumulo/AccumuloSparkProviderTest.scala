/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.spark.accumulo

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.geomesa.testcontainers.AccumuloContainer
import org.geomesa.testcontainers.spark.SparkCluster
import org.geotools.api.data.{DataStoreFinder, Query, SimpleFeatureStore, Transaction}
import org.geotools.api.feature.simple.SimpleFeature
import org.geotools.data.DataUtilities
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.specification.BeforeAfterAll
import org.testcontainers.containers.Network

class AccumuloSparkProviderTest extends SpecificationWithJUnit with BeforeAfterAll with StrictLogging {

  import org.locationtech.geomesa.filter.ff
  import org.locationtech.geomesa.spark.jts._

  import scala.collection.JavaConverters._

  private val network = Network.newNetwork()

  private val accumulo =
    new AccumuloContainer()
      .withGeoMesaDistributedRuntime()
      .withNetwork(network)

  private val cluster =
    new SparkCluster()
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

  // TODO enforce only a single instance at once
  lazy val spark: SparkSession = cluster.getOrCreateSession().withJTS
  lazy val sc: SQLContext = spark.sqlContext.withJTS // <-- withJTS should be a noop given the above, but is here to test that code path
  lazy val sparkContext: SparkContext = spark.sparkContext

  lazy val df: DataFrame =
    spark.read
      .format("geomesa")
      .options(sparkDsParams)
      .option("geomesa.feature", "chicago")
      .load()

  /**
   * Constructor for creating a DataFrame with a single row and no columns.
   * Useful for testing the invocation of data constructing UDFs.
   */
  def dfBlank(): DataFrame = {
    // This is to enable us to do a single row creation select operation in DataFrame
    // world. Probably a better/easier way of doing this.
    spark.createDataFrame(spark.sparkContext.makeRDD(Seq(Row())), StructType(Seq.empty))
  }

  override def beforeAll(): Unit = {
    // note: the host reach-back networking required for spark seems to mess up the accumulo networking unless accumulo starts first
    accumulo.start()
    cluster.start()

    // ingest some data
    val sft = SimpleFeatureTypes.createType("chicago", "arrest:String,case_number:Int:index=full,dtg:Date,*geom:Point:srid=4326")

    ds.createSchema(sft)

    val features = List[SimpleFeature](
      ScalaSimpleFeature.create(sft, "1", "true", "1", "2016-01-01T00:00:00.000Z", "POINT (-76.5 38.5)"),
      ScalaSimpleFeature.create(sft, "2", "true", "2", "2016-01-02T00:00:00.000Z", "POINT (-77.0 38.0)"),
      ScalaSimpleFeature.create(sft, "3", "true", "3", "2016-01-03T00:00:00.000Z", "POINT (-78.0 39.0)"),
    )

    features.foreach(_.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE))

    ds.getFeatureSource("chicago").asInstanceOf[SimpleFeatureStore].addFeatures(DataUtilities.collection(features.asJava))

    logger.debug(df.schema.treeString)
    df.createOrReplaceTempView("chicago")
  }

  override def afterAll(): Unit = {
    CloseWithLogging(cluster)
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
