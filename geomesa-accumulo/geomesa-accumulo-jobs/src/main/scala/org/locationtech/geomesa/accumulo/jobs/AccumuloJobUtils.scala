/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.jobs

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.AccumuloClient
import org.apache.hadoop.conf.Configuration
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.jobs.JobUtils
import org.locationtech.geomesa.utils.classpath.ClassPathUtils

import java.io.File
import scala.io.Source
import scala.util.Try

object AccumuloJobUtils extends LazyLogging {

  // default jars that will be included with m/r jobs
  lazy val defaultLibJars: List[String] = {
    val defaultLibJarsFile = "org/locationtech/geomesa/jobs/accumulo-libjars.list"
    val url = Try(getClass.getClassLoader.getResource(defaultLibJarsFile))
    val source = url.map(Source.fromURL)
    val lines = source.map(_.getLines().toList)
    source.foreach(_.close())
    lines.get
  }

  // paths are in order of preference for finding a jar
  def defaultSearchPath: Iterator[() => Seq[File]] =
    Iterator(() => ClassPathUtils.getJarsFromEnvironment("GEOMESA_ACCUMULO_HOME", "lib"),
             () => ClassPathUtils.getJarsFromEnvironment("ACCUMULO_HOME"),
             () => ClassPathUtils.getJarsFromClasspath(classOf[AccumuloDataStore]),
             () => ClassPathUtils.getJarsFromClasspath(classOf[AccumuloClient]))

  /**
   * Sets the libjars into a Hadoop configuration. Will search the environment first, then the
   * classpath, until all required jars have been found.
   *
   * @param conf job configuration
   * @param libJars jar prefixes to load
   */
  def setLibJars(conf: Configuration,
                 libJars: Seq[String] = defaultLibJars,
                 searchPath: Iterator[() => Seq[File]] = defaultSearchPath): Unit =
    JobUtils.setLibJars(conf, libJars, searchPath)
}
