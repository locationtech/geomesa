/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs

import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.conf.Configuration
import org.geomesa.testcontainers.HadoopContainer
import org.testcontainers.utility.DockerImageName

import java.io.{ByteArrayInputStream, StringWriter}
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicBoolean
import scala.util.Try

/**
 * Hadoop cluster for testing. Singleton object that is shared between all test classes in the jvm.
 */
object HadoopSharedCluster extends StrictLogging {

  val ImageName =
    DockerImageName.parse("ghcr.io/geomesa/accumulo-uno")
        .withTag(sys.props.getOrElse("accumulo.docker.tag", "2.1.3"))

  lazy val Container: HadoopContainer = tryContainer.get

  lazy val ContainerConfiguration: Configuration = {
    val conf = new Configuration(false)
    conf.addResource(new ByteArrayInputStream(Container.getConfigurationXml.getBytes(StandardCharsets.UTF_8)), "")
    conf.set("parquet.compression", "GZIP", "") // default is snappy which is not on our classpath
    conf
  }

  lazy val ContainerConfig: String = {
    val writer = new StringWriter()
    ContainerConfiguration.writeXml(writer)
    writer.toString
  }

  private lazy val tryContainer: Try[HadoopContainer] = Try {
    logger.info("Starting Hadoop container")
    val container = new HadoopContainer(ImageName)
    initialized.getAndSet(true)
    container.start()
    logger.info("Started Hadoop container")
    container
  }

  private val initialized = new AtomicBoolean(false)

  sys.addShutdownHook({
    if (initialized.get) {
      logger.info("Stopping Hadoop container")
      tryContainer.foreach(_.stop())
      logger.info("Stopped Hadoop container")
    }
  })
}
