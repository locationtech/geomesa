/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.hbase.data

import com.typesafe.scalalogging.LazyLogging
import org.geomesa.testcontainers.hbase.HBaseContainer
import org.testcontainers.utility.DockerImageName

import scala.util.Try

/**
 * Singleton HBase cluster used for testing
 */
object HBaseCluster extends LazyLogging {

  val ImageName =
    DockerImageName.parse("ghcr.io/geomesa/hbase-docker")
      .withTag(sys.props.getOrElse("hbase.docker.tag", "2.6.3-jdk17"))

  private val container = new HBaseContainer(ImageName)

  private lazy val cluster = Try {
    container.withGeoMesaDistributedRuntime().withSecurityEnabled().start()
    container
  }

  lazy val Container: HBaseContainer = cluster.get

  /**
   * The hbase-site.xml contents required to connect to this cluster
   */
  lazy val hbaseSiteXml = Container.getHBaseSiteXml

  sys.addShutdownHook(container.stop())
}
