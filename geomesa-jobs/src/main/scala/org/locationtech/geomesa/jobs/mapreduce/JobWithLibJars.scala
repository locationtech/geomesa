/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.jobs.mapreduce

import java.io.File
import java.nio.charset.StandardCharsets

import org.apache.commons.io.IOUtils
import org.apache.hadoop.mapreduce.Job
import org.locationtech.geomesa.jobs.JobUtils
import org.locationtech.geomesa.utils.classpath.ClassPathUtils

import scala.util.control.NonFatal

trait JobWithLibJars {

  def setLibJars(job: Job, fileName: String, searchPath: Iterator[() => Seq[File]]): Unit =
    JobUtils.setLibJars(job.getConfiguration, readLibJars(fileName), defaultSearchPath ++ searchPath)

  private def defaultSearchPath: Iterator[() => Seq[File]] =
    Iterator(
      () => ClassPathUtils.getJarsFromClasspath(getClass),
      () => ClassPathUtils.getFilesFromSystemProperty("geomesa.convert.scripts.path")
    )

  private def readLibJars(file: String): Seq[String] = {
    val is = getClass.getClassLoader.getResourceAsStream(file)
    try {
      import scala.collection.JavaConversions._
      IOUtils.readLines(is, StandardCharsets.UTF_8)
    } catch {
      case NonFatal(e) => throw new Exception("Error reading ingest libjars", e)
    } finally {
      IOUtils.closeQuietly(is)
    }
  }

}
