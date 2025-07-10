/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.redis.data

import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.data.{DataStoreFinder, Query, Transaction}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.data.DataUtilities
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.ReferencedEnvelope
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.utils.ExplainString
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs
import org.locationtech.geomesa.utils.geotools.{CRS_EPSG_4326, FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.containers.{FixedHostPortGenericContainer, GenericContainer}
import org.testcontainers.utility.DockerImageName
import redis.clients.jedis.UnifiedJedis

import java.util.concurrent.TimeUnit
import java.util.{Collections, Date}
import scala.concurrent.duration.DurationInt

@RunWith(classOf[JUnitRunner])
class RedisDataStoreClusterTest extends Specification with LazyLogging {

  sequential
  import scala.collection.JavaConverters._

  var container: GenericContainer[_] = _

  val sft = SimpleFeatureTypes.createImmutableType("test", "name:String:index=true,dtg:Date,*geom:Point:srid=4326")

  def features(sft: SimpleFeatureType = sft): Seq[ScalaSimpleFeature] = Seq.tabulate(10) { i =>
    ScalaSimpleFeature.create(sft, i.toString, s"name$i", s"2019-01-03T0$i:00:00.000Z", s"POINT (-4$i 55)")
  }

  val filters = Seq(
    "bbox(geom, -39, 54, -51, 56)",
    "bbox(geom, -45, 54, -49, 56)",
    "bbox(geom, -39, 54, -51, 56) AND dtg >= '2019-01-03T00:00:00.000Z' AND dtg < '2019-01-03T12:00:00.000Z'",
    "bbox(geom, -45, 54, -49, 56) AND dtg >= '2019-01-03T00:00:00.000Z' AND dtg < '2019-01-03T12:00:00.000Z'",
    "bbox(geom, -39, 54, -51, 56) AND dtg during 2019-01-03T04:30:00.000Z/2019-01-03T08:30:00.000Z",
    s"name IN('${features().map(_.getAttribute("name")).mkString("', '")}')",
    "name IN('name0', 'name2') AND dtg >= '2019-01-03T00:00:00.000Z' AND dtg < '2019-01-03T01:00:00.000Z'",
    features().map(_.getID).mkString("IN('", "', '", "')")
  ).map(ECQL.toFilter)

  val transforms = Seq(null, Array("dtg", "geom"), Array("name", "geom"))

  // Using container addresses instead of localhost
  lazy val params: Map[String, String] = Map(
    RedisDataStoreParams.RedisUrlParam.key ->
      s"redis://127.0.0.1:6379,redis://127.0.0.1:6380,redis://127.0.0.1:6381",
    RedisDataStoreParams.RedisClusterParam.key -> "true",
    RedisDataStoreParams.RedisCatalogParam.key -> "gm-test",
    RedisDataStoreParams.PipelineParam.key -> "false"
  )

  step {
    // Define common image and configuration
    val image = DockerImageName.parse("redis").withTag(sys.props.getOrElse("redis.docker.cluster.tag", "7.0-alpine"))

    // Create Redis nodes using Java-style builder approach
    node1 = {
      val c = new FixedHostPortGenericContainer(image.asCanonicalNameString())
      c.withNetworkMode("host")
      c.withNetworkAliases("redis-node-1")
      c.withFixedExposedPort(6379, 6379)
      c.withCommand(
        "redis-server",
        "--port", "6379",
        "--cluster-enabled", "yes",
        "--cluster-node-timeout", "5000",
        "--cluster-announce-ip", "127.0.0.1",
        "--appendonly", "yes",
        "--bind", "0.0.0.0",
        "--protected-mode", "no"
      )
      c
    }

    node2 = {
      val c = new FixedHostPortGenericContainer(image.asCanonicalNameString())
      c.withNetworkMode("host")
      c.withNetworkAliases("redis-node-2")
      c.withFixedExposedPort(6380, 6380)
      c.withCommand(
        "redis-server",
        "--port", "6380",
        "--cluster-enabled", "yes",
        "--cluster-node-timeout", "5000",
        "--cluster-announce-ip", "127.0.0.1",
        "--appendonly", "yes",
        "--bind", "0.0.0.0",
        "--protected-mode", "no"
      )
      c
    }

    node3 = {
      val c = new FixedHostPortGenericContainer(image.asCanonicalNameString())
      c.withNetworkMode("host")
      c.withNetworkAliases("redis-node-3")
      c.withFixedExposedPort(6381, 6381)
      c.withCommand(
        "redis-server",
        "--port", "6381",
        "--cluster-enabled", "yes",
        "--cluster-node-timeout", "5000",
        "--cluster-announce-ip", "127.0.0.1",
        "--appendonly", "yes",
        "--bind", "0.0.0.0",
        "--protected-mode", "no"
      )
      c
    }

    // Start the containers
    node1.start()
    node2.start()
    node3.start()

    // Follow logs for debugging
    node1.followOutput(new Slf4jLogConsumer(logger.underlying))
    node2.followOutput(new Slf4jLogConsumer(logger.underlying))
    node3.followOutput(new Slf4jLogConsumer(logger.underlying))

    // Initialize cluster - this runs redis-cli to create the cluster after all nodes are started
    val initCommand = Array(
      "redis-cli", "--cluster", "create",
      s"127.0.0.1:6379",
      s"127.0.0.1:6380",
      s"127.0.0.1:6381",
      "--cluster-replicas", "0",
      "--cluster-yes"
    )

    val execResult = node1.execInContainer(initCommand: _*)
    logger.info(s"Redis cluster initialization result: ${execResult.getExitCode}")
    logger.info(s"Redis cluster initialization stdout: ${execResult.getStdout}")
    logger.info(s"Redis cluster initialization stderr: ${execResult.getStderr}")

    // Verify cluster is properly formed
    val checkCommand = Array("redis-cli", "cluster", "info")
    val checkResult = node1.execInContainer(checkCommand: _*)
    logger.info(s"Redis cluster status: ${checkResult.getStdout}")

    // Wait for the cluster to be ready by polling until the cluster state is 'ok'
    var isClusterReady = false
    val startTime = System.currentTimeMillis()
    val timeout = 30000 // 30 seconds timeout
    while (!isClusterReady && System.currentTimeMillis() - startTime < timeout) {
      val clusterInfo = node1.execInContainer("redis-cli", "cluster", "info").getStdout
      if (clusterInfo.contains("cluster_state:ok")) {
        isClusterReady = true
        logger.info("Redis cluster is ready!")
      } else {
        logger.info("Waiting for Redis cluster to be ready...")
        TimeUnit.SECONDS.sleep(1)
      }
    }

    if (!isClusterReady) {
      throw new RuntimeException("Redis cluster failed to initialize within timeout period")
    }

    // Make sure all slots are assigned
    val slotsCommand = Array("redis-cli", "cluster", "slots")
    val slotsResult = node1.execInContainer(slotsCommand: _*)
    logger.info(s"Redis cluster slots: ${slotsResult.getStdout}")

    // Store the first node as our main container for test use
    container = node1
  }

  def featureType(name: String, ageOff: Option[String]): SimpleFeatureType =
    SimpleFeatureTypes.immutable(SimpleFeatureTypes.renameSft(sft, name),
      ageOff.map(Collections.singletonMap(Configs.FeatureExpiration, _)).orNull)

  "RedisDataStore" should {
    "read and write features" in {

      val ds = DataStoreFinder.getDataStore(params.asJava).asInstanceOf[RedisDataStore]
      ds must not(beNull)

      try {
        ds.getSchema(sft.getTypeName) must beNull
        ds.createSchema(sft)
        ds.getSchema(sft.getTypeName) mustEqual sft

        val features = this.features(sft)
        WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          features.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
        }

        foreach(filters) { filter =>
          val filtered = features.filter(filter.evaluate)
          foreach(transforms) { transform =>
            val query = new Query(sft.getTypeName, filter, transform: _*)
            val result = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
            val expected = if (transform == null) { filtered } else {
              val tsft = DataUtilities.createSubType(sft, transform: _*)
              filtered.map(DataUtilities.reType(tsft, _)).map(ScalaSimpleFeature.copy)
            }
            result must containTheSameElementsAs(expected)
          }
        }

        ds.stats.getCount(sft) must beSome(10L)
        ds.stats.getBounds(sft) mustEqual new ReferencedEnvelope(-49, -40, 55, 55, CRS_EPSG_4326)
      } finally {
        ds.removeSchema(sft.getTypeName)
        ds.dispose()
      }
    }

    "expire features based on ingest time" in {
      val sft = featureType("ingest", Some("2 seconds"))

      RedisSystemProperties.AgeOffInterval.threadLocalValue.set("100 ms")
      val ds = try { DataStoreFinder.getDataStore(params.asJava).asInstanceOf[RedisDataStore] } finally {
        RedisSystemProperties.AgeOffInterval.threadLocalValue.remove()
      }
      ds must not(beNull)

      try {
        ds.getSchema(sft.getTypeName) must beNull
        ds.createSchema(sft)
        ds.getSchema(sft.getTypeName) mustEqual sft

        val features = this.features(sft)
        WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          features.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
        }

        foreach(filters) { filter =>
          val expected = features.filter(filter.evaluate)
          val query = new Query(sft.getTypeName, filter)
          val result = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
          result must containTheSameElementsAs(expected)
        }

        ds.stats.getCount(sft) must beSome(10L)
        ds.stats.getBounds(sft) mustEqual new ReferencedEnvelope(-49, -40, 55, 55, CRS_EPSG_4326)

        foreach(filters) { filter =>
          val query = new Query(sft.getTypeName, filter)
          eventually(10, 1000.millis) {
            val result = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
            result must beEmpty
          }
        }

        ds.stats.getCount(sft) must beSome(0L)
      } finally {
        ds.removeSchema(sft.getTypeName)
        ds.dispose()
      }
    }

    "expire features based on attribute time" in {

      // age off the first feature, since they are one hour apart
      val time = System.currentTimeMillis() + 2000L - features().head.getAttribute("dtg").asInstanceOf[Date].getTime

      val sft = featureType("time", Some(s"dtg($time ms)"))

      RedisSystemProperties.AgeOffInterval.threadLocalValue.set("100 ms")
      val ds = try { DataStoreFinder.getDataStore(params.asJava).asInstanceOf[RedisDataStore] } finally {
        RedisSystemProperties.AgeOffInterval.threadLocalValue.remove()
      }
      ds must not(beNull)

      try {
        ds.getSchema(sft.getTypeName) must beNull
        ds.createSchema(sft)
        ds.getSchema(sft.getTypeName) mustEqual sft

        val features = this.features(sft)

        WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          features.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
        }

        foreach(filters) { filter =>
          val expected = features.filter(filter.evaluate)
          val query = new Query(sft.getTypeName, filter)
          val result = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
          result must containTheSameElementsAs(expected)
        }

        ds.stats.getCount(sft) must beSome(10L)
        ds.stats.getBounds(sft) mustEqual new ReferencedEnvelope(-49, -40, 55, 55, CRS_EPSG_4326)

        foreach(filters) { filter =>
          val expected = features.drop(1).filter(filter.evaluate)
          val query = new Query(sft.getTypeName, filter)
          eventually(10, 1000.millis) {
            val result = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
            result must containTheSameElementsAs(expected)
          }
        }

        ds.stats.getCount(sft) must beSome(9L)
      } finally {
        ds.removeSchema(sft.getTypeName)
        ds.dispose()
      }
    }

    "generate explain plans" in {
      val sft = featureType("explain", None)

      val ds = DataStoreFinder.getDataStore(params.asJava).asInstanceOf[RedisDataStore]
      ds must not(beNull)
      try {
        ds.createSchema(sft)
        val explain = new ExplainString()
        ds.getQueryPlan(new Query(sft.getTypeName), explainer = explain) must not(throwAn[Exception])
        // TODO GEOMESA-3035 remove this check when we implement a better work-around
        explain.toString must contain("unserializable state=???")
      } finally {
        ds.dispose()
      }
    }

    "should dispose of connections" in {
      val ds = DataStoreFinder.getDataStore(params.asJava).asInstanceOf[RedisDataStore]
      ds must not(beNull)

      // show that the cluster connection works prior to disposal
      ds.connection.getResource.asInstanceOf[UnifiedJedis].ping() mustEqual "PONG"
      ds.dispose()
      // show that the cluster connection is closed after
      ds.connection.getResource.asInstanceOf[UnifiedJedis].ping() must beNull
      ds.connection.isClosed must beTrue
    }
  }

  var node1: GenericContainer[_] = _
  var node2: GenericContainer[_] = _
  var node3: GenericContainer[_] = _

  step {
    try {
      // Stop all nodes and free resources
      if (node1 != null) { node1.stop() }
      if (node2 != null) { node2.stop() }
      if (node3 != null) { node3.stop() }
    } catch {
      case e: Exception => logger.error("Error stopping containers", e)
    }
  }
}
