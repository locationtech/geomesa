/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.geotools.spark

import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.geomesa.testcontainers.spark.SparkCluster
import org.geotools.api.data.{DataStoreFinder, Query, Transaction}
import org.geotools.api.feature.simple.SimpleFeature
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.geotools.spark.GeoToolsSpatialRDDProviderIT.PostgisContainer
import org.locationtech.geomesa.spark.GeoMesaSpark
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.{CloseWithLogging, WithClose}
import org.slf4j.LoggerFactory
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.specification.BeforeAfterAll
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.containers.{GenericContainer, Network}
import org.testcontainers.lifecycle.Startables
import org.testcontainers.utility.DockerImageName

import java.nio.file.{Files, Path}
import java.util.Collections

class GeoToolsSpatialRDDProviderIT extends SpecificationWithJUnit with BeforeAfterAll with StrictLogging {

  import org.locationtech.geomesa.spark.jts._

  import scala.collection.JavaConverters._

  private val sparkRuntimeJar =
    WithClose(Files.newDirectoryStream(Path.of(sys.props("maven.target.dir")), "geomesa-gt-spark*-runtime.jar")) { files =>
      val iter = files.iterator()
      if (iter.hasNext) {
        iter.next().toFile
      } else {
        throw new RuntimeException("Could not find spark-runtime jar")
      }
    }

  val network = Network.newNetwork()

  val cluster = new SparkCluster(Collections.singleton(sparkRuntimeJar)).withNetwork(network)

  val postgis: PostgisContainer =
    new PostgisContainer()
      .withNetwork(network)
      .withNetworkAliases("postgres")

  lazy val postgisParams = Map(
    "dbtype" -> "postgis",
    "host" -> postgis.getHost,
    "port" -> s"${postgis.getMappedPort(5432)}",
    "database" -> "postgres",
    "user" -> "postgres",
    "passwd" -> "postgres",
    "Batch insert size" -> "10",
    "preparedStatements" -> "true",
    "geotools" -> "true", // required for the geomesa spark provider
  )

  lazy val postgisSparkParams = postgisParams ++ Map("host" -> "postgres", "port" -> "5432")

  override def beforeAll(): Unit = Startables.deepStart(cluster, postgis).get()

  override def afterAll(): Unit = CloseWithLogging(cluster, postgis)

  // TODO enforce only a single instance at once
  lazy val spark: SparkSession = cluster.getOrCreateSession().withJTS
  lazy val sc: SQLContext = spark.sqlContext.withJTS // <-- withJTS should be a noop given the above, but is here to test that code path
  lazy val sparkContext: SparkContext = spark.sparkContext

  lazy val chicagoSft =
    SimpleFeatureTypes.createType("chicago",
      "arrest:String,case_number:Int,dtg:Date,*geom:Point:srid=4326")

  lazy val chicagoFeatures: Seq[SimpleFeature] = Seq(
    ScalaSimpleFeature.create(chicagoSft, "1", "true", 1, "2016-01-01T00:00:00.000Z", "POINT (-76.5 38.5)"),
    ScalaSimpleFeature.create(chicagoSft, "2", "true", 2, "2016-01-02T00:00:00.000Z", "POINT (-77.0 38.0)"),
    ScalaSimpleFeature.create(chicagoSft, "3", "true", 3, "2016-01-03T00:00:00.000Z", "POINT (-78.0 39.0)"),
    ScalaSimpleFeature.create(chicagoSft, "4", "true", 4, "2016-01-01T00:00:00.000Z", "POINT (-73.5 39.5)"),
    ScalaSimpleFeature.create(chicagoSft, "5", "true", 5, "2016-01-02T00:00:00.000Z", "POINT (-74.0 35.5)"),
    ScalaSimpleFeature.create(chicagoSft, "6", "true", 6, "2016-01-03T00:00:00.000Z", "POINT (-79.0 37.5)")
  )

  "The GeoToolsSpatialRDDProvider" should {
    "read from a database" in {
      val sft = SimpleFeatureTypes.renameSft(chicagoSft, "chicago_read")
      WithClose(DataStoreFinder.getDataStore(postgisParams.asJava)) { ds =>
        ds.createSchema(sft)
        WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          chicagoFeatures.take(3).foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
        }

        val rdd = GeoMesaSpark(postgisParams).rdd(new Configuration(), sparkContext, postgisParams, new Query(sft.getTypeName))
        rdd.count() mustEqual 3L
      }
    }

    "write to a database" in {
      val sft = SimpleFeatureTypes.renameSft(chicagoSft, "chicago_write")
      WithClose(DataStoreFinder.getDataStore(postgisParams.asJava)) { ds =>
        ds.createSchema(sft)
        val writeRdd = sparkContext.parallelize(chicagoFeatures)
        // need to bypass the store check up fron b/c ds params are different from the local check and the distributed write
        GeoToolsSpatialRDDProvider.StoreCheck.threadLocalValue.set("false")
        try {
          GeoMesaSpark(postgisParams).save(writeRdd, postgisSparkParams, sft.getTypeName)
        } finally {
          GeoToolsSpatialRDDProvider.StoreCheck.threadLocalValue.remove()
        }
        // verify write
        val readRdd = GeoMesaSpark(postgisParams.asJava).rdd(new Configuration(), sparkContext, postgisParams, new Query(sft.getTypeName))
        readRdd.count() mustEqual 6L
      }
    }
  }
}

object GeoToolsSpatialRDDProviderIT {

  val PostgisImage =
    DockerImageName.parse("ghcr.io/geomesa/postgis-cron")
      .withTag(sys.props.getOrElse("postgis.docker.tag", "15-3.4"))

  class PostgisContainer extends GenericContainer[PostgisContainer](PostgisImage) {
    withEnv("POSTGRES_PASSWORD", "postgres")
    withExposedPorts(5432)
    withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("postgis")))
    waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*", 2))
  }
}
