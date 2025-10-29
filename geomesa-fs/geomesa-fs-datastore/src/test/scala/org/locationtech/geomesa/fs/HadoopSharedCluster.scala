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

import java.io.{ByteArrayInputStream, StringWriter}
import java.nio.charset.StandardCharsets

/**
 * Hadoop cluster for testing. Singleton object that is shared between all test classes in the jvm.
 */
object HadoopSharedCluster extends StrictLogging {

  lazy val Container: HadoopContainer = HadoopContainer.getInstance()

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
}
