/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.spark

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.slf4j.LoggerFactory
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.specification.BeforeAfterAll
import org.testcontainers.Testcontainers
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.containers.{BindMode, GenericContainer, Network}
import org.testcontainers.lifecycle.Startables
import org.testcontainers.utility.DockerImageName

import java.net.{ServerSocket, URI}
import java.nio.file.Paths

trait TestWithSpark extends SpecificationWithJUnit with BeforeAfterAll with StrictLogging {

  import org.locationtech.geomesa.spark.jts._

  import scala.collection.JavaConverters._

  val network = Network.newNetwork()

  private val driverPort = TestWithSpark.getFreePort
  private val blockManagerPort = TestWithSpark.getFreePort

  logger.debug(s"Driver port: $driverPort")
  logger.debug(s"BlockManager port: $blockManagerPort")

  val master =
    new SparkContainer()
      .withNetwork(network)
      .withNetworkAliases("spark-master")
      .withExposedPorts(7077)
      .withEnv("SPARK_LOCAL_IP", "spark-master")
      .withCommand("/opt/spark/bin/spark-class", "org.apache.spark.deploy.master.Master")
      .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("spark-master")))

  val worker =
    new SparkContainer()
      .withNetwork(network)
      .withNetworkAliases("spark-worker")
      .withEnv("SPARK_WORKER_CORES", sys.props.getOrElse("spark.worker.cores", "2"))
      .withEnv("SPARK_WORKER_MEMORY", sys.props.getOrElse("spark.worker.memory", "1g"))
      .withCommand("/opt/spark/bin/spark-class", "org.apache.spark.deploy.worker.Worker", "spark://spark-master:7077")
      .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("spark-worker")))
      .withAccessToHost(true) // needed to call back to the driver running locally
      .dependsOn(master)

  // load our spark-runtime jars into the worker classpath - this props file comes from the shaded geomesa-utils jar
  getClass.getClassLoader.getResources("org/locationtech/geomesa/geomesa.properties").asScala.foreach { url =>
    val uri = url.toURI
    if ("jar".equals(uri.getScheme) && uri.toString.contains("spark-runtime")) {
      // uris look like: jar://file://foo.jar!/path/in/jar
      val jar = uri.toString.substring(4).replaceAll("\\.jar!.*", ".jar")
      val jarFile = Paths.get(URI.create(jar)).toFile
      val hostPath = jarFile.getAbsolutePath
      logger.info(s"Mounting spark-runtime jar: $hostPath")
      worker.withFileSystemBind(hostPath, s"/opt/spark/jars/${jarFile.getName}", BindMode.READ_ONLY)
    }
  }

  // TODO enforce only a single instance at once
  lazy val spark: SparkSession = {
    Testcontainers.exposeHostPorts(driverPort, blockManagerPort)
    SparkSession.builder()
      .appName("test")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", "org.locationtech.geomesa.spark.GeoMesaSparkKryoRegistrator")
      .config("spark.sql.crossJoin.enabled", "true")
      .config("spark.ui.enabled", value = false)
      .config("spark.driver.bindAddress", "0.0.0.0")
      .config("spark.driver.host", "host.testcontainers.internal") // so that the cluster can communicate back to the driver
      .config("spark.driver.port", driverPort)
      .config("spark.driver.blockManager.port", blockManagerPort)
      .master(s"spark://${master.getHost}:${master.getMappedPort(7077)}")
      .getOrCreate()
      .withJTS
  }
  lazy val sc: SQLContext = spark.sqlContext.withJTS // <-- withJTS should be a noop given the above, but is here to test that code path
  lazy val sparkContext: SparkContext = spark.sparkContext

  override def beforeAll(): Unit = Startables.deepStart(master, worker).get()

  override def afterAll(): Unit = CloseWithLogging(Seq(spark, worker, master))

  /**
   * Constructor for creating a DataFrame with a single row and no columns.
   * Useful for testing the invocation of data constructing UDFs.
   */
  def dfBlank(): DataFrame = {
    // This is to enable us to do a single row creation select operation in DataFrame
    // world. Probably a better/easier way of doing this.
    spark.createDataFrame(spark.sparkContext.makeRDD(Seq(Row())), StructType(Seq.empty))
  }

  class SparkContainer extends GenericContainer[SparkContainer](TestWithSpark.ImageName)
}

object TestWithSpark {

  val ImageName =
    DockerImageName.parse("apache/spark").withTag(sys.props.getOrElse("spark.docker.tag", "3.5.7-scala2.12-java17-ubuntu"))

  private def getFreePort: Int = {
    val socket = new ServerSocket(0)
    try { socket.getLocalPort } finally {
      socket.close()
    }
  }
}
