/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.spark

import java.nio.file.{Files, Path}

import com.datastax.driver.core.{Cluster, SocketOptions}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.geotools.data.{DataStoreFinder, Query, Transaction}
import org.junit.runner.RunWith
import org.locationtech.geomesa.cassandra.data.CassandraDataStore
import org.locationtech.geomesa.cassandra.data.CassandraDataStoreFactory.Params
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.spark.{GeoMesaSpark, GeoMesaSparkKryoRegistrator, SparkSQLTestUtils}
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class CassandraSparkProviderTest extends Specification {

  sequential

  var storage: Path = _
  var params: Map[String, String] = _
  var ds: CassandraDataStore = _

  var sc: SparkContext = _
  var spark: SparkSession = _
  var sql: SQLContext = _

  step {
    // cassandra database set up
    storage = Files.createTempDirectory("cassandra")

    System.setProperty("cassandra.storagedir", storage.toString)

    EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra-config.yaml", 1200000L)

    var readTimeout: Int = SystemProperty("cassandraReadTimeout", "12000").get.toInt

    if (readTimeout < 0) {
      readTimeout = 12000
    }
    val host = EmbeddedCassandraServerHelper.getHost
    val port = EmbeddedCassandraServerHelper.getNativeTransportPort
    val cluster = new Cluster.Builder().addContactPoints(host).withPort(port)
      .withSocketOptions(new SocketOptions().setReadTimeoutMillis(readTimeout)).build().init()
    val session = cluster.connect()
    val cqlDataLoader = new CQLDataLoader(session)
    cqlDataLoader.load(new ClassPathCQLDataSet("init.cql", false, false))

    // add parameters to point to Cassandra
    params = Map(
      Params.ContactPointParam.getName -> s"$host:$port",
      Params.KeySpaceParam.getName -> "geomesa_cassandra",
      Params.CatalogParam.getName -> "test_sft",
      "geomesa.cassandra.host" -> s"$host"
    )
    ds = DataStoreFinder.getDataStore(params).asInstanceOf[CassandraDataStore]

    spark = org.locationtech.geomesa.spark.SparkSQLTestUtils.createSparkSession()
    sc = spark.sparkContext
    sql = spark.sqlContext

    SparkSQLTestUtils.ingestChicago(ds)

    val df = spark.read
      .format("geomesa")
      .options(params)
      .option("geomesa.feature", "chicago")
      .load()
//    logger.debug(df.schema.treeString)
    df.createOrReplaceTempView("chicago")
    // Spark set up
//    val conf = new SparkConf().setMaster("local[2]").setAppName("testSpark")
//    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    conf.set("spark.kryo.registrator", classOf[GeoMesaSparkKryoRegistrator].getName)
//    sc = SparkContext.getOrCreate(conf)
  }

  // Define feature schema
  lazy val chicagoSft =
    SimpleFeatureTypes.createType("chicago",
      "arrest:String,case_number:Int,dtg:Date,*geom:Point:srid=4326")

  // Make test features
  lazy val chicagoFeatures: Seq[SimpleFeature] = Seq(
    ScalaSimpleFeature.create(chicagoSft, "1", "true", 1, "2016-01-01T00:00:00.000Z", "POINT (-76.5 38.5)"),
    ScalaSimpleFeature.create(chicagoSft, "2", "true", 2, "2016-01-02T00:00:00.000Z", "POINT (-77.0 38.0)"),
    ScalaSimpleFeature.create(chicagoSft, "3", "true", 3, "2016-01-03T00:00:00.000Z", "POINT (-78.0 39.0)"),
    ScalaSimpleFeature.create(chicagoSft, "4", "true", 4, "2016-01-01T00:00:00.000Z", "POINT (-73.5 39.5)"),
    ScalaSimpleFeature.create(chicagoSft, "5", "true", 5, "2016-01-02T00:00:00.000Z", "POINT (-74.0 35.5)"),
    ScalaSimpleFeature.create(chicagoSft, "6", "true", 6, "2016-01-03T00:00:00.000Z", "POINT (-79.0 37.5)")
  )

  "The CassandraSpatialRDDProvider" should {
    "read from the embedded Cassandra database" in {
      val ds = DataStoreFinder.getDataStore(params)
      ds.createSchema(chicagoSft)
      WithClose(ds.getFeatureWriterAppend("chicago", Transaction.AUTO_COMMIT)) { writer =>
        chicagoFeatures.take(3).foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
      }

      val rdd = GeoMesaSpark(params).rdd(new Configuration(), sc, params, new Query("chicago"))
      rdd.count() mustEqual 3l
    }

    "write to the embedded Cassandra database" in {
      val ds = DataStoreFinder.getDataStore(params)
      ds.createSchema(chicagoSft)
      val writeRdd = sc.parallelize(chicagoFeatures)
      GeoMesaSpark(params).save(writeRdd, params, "chicago")
      // verify write
      val readRdd = GeoMesaSpark(params).rdd(new Configuration(), sc, params, new Query("chicago"))
      readRdd.count() mustEqual 6l
    }

    "select by secondary indexed attribute" >> {
      val df = spark.read
        .format("geomesa")
        .options(params)
        .option("geomesa.feature", "chicago")
        .load()
      val cases = df.select("case_number").where("case_number = 1").collect().map(_.getInt(0))
      cases.length mustEqual 1
    }

    "complex st_buffer" >> {
      val buf = sql.sql("select st_asText(st_bufferPoint(geom,10)) from chicago where case_number = 1").collect().head.getString(0)
      sql.sql(
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
