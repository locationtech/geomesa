/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.jobs.mapreduce

import java.io.{File, IOException}
import java.nio.charset.StandardCharsets

import org.apache.commons.io.IOUtils
import org.apache.hadoop.mapreduce.Job
import org.locationtech.geomesa.jobs.JobUtils
import org.locationtech.geomesa.utils.io.WithClose

import scala.util.control.NonFatal

trait JobWithLibJars {

  import scala.collection.JavaConverters._

  def setLibJars(job: Job, fileNames: Seq[String], searchPath: Iterator[() => Seq[File]]): Unit =
    JobUtils.setLibJars(job.getConfiguration, fileNames.flatMap(readLibJars), searchPath)

  private def readLibJars(file: String): Seq[String] = {
    try {
      WithClose(getClass.getClassLoader.getResourceAsStream(file)) { is =>
        IOUtils.readLines(is, StandardCharsets.UTF_8).asScala
      }
    } catch {
      case NonFatal(e) => throw new IOException(s"Error reading ingest libjars '$file'", e)
    }
  }
}
