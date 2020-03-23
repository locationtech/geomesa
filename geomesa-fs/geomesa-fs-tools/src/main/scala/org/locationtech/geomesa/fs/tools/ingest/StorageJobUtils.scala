/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.ingest

import java.util.Collections

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.JobStatus
import org.apache.hadoop.tools.{DistCp, DistCpOptions}
import org.locationtech.geomesa.tools.Command
import org.locationtech.geomesa.tools.utils.StatusCallback

object StorageJobUtils extends LazyLogging {

  def distCopy(srcRoot: Path, destRoot: Path, statusCallback: StatusCallback): Boolean = {
    statusCallback.reset()

    Command.user.info("Submitting DistCp job - please wait...")

    val opts = distCpOptions(srcRoot, destRoot)
    val job = new DistCp(new Configuration, opts).execute()

    Command.user.info(s"Tracking available at ${job.getStatus.getTrackingUrl}")

    // distCp has no reduce phase
    while (!job.isComplete) {
      if (job.getStatus.getState != JobStatus.State.PREP) {
        statusCallback(s"DistCp: ", job.mapProgress(), Seq.empty, done = false)
      }
      Thread.sleep(1000)
    }
    statusCallback(s"DistCp: ", job.mapProgress(), Seq.empty, done = true)

    val success = job.isSuccessful
    if (success) {
      Command.user.info(s"Successfully copied data to $destRoot")
    } else {
      Command.user.error(s"Failed to copy data to $destRoot")
    }
    success
  }

  private def distCpOptions(src: Path, dest: Path): DistCpOptions =
    try { distCpOptions3(src, dest) } catch { case _: ClassNotFoundException => distCpOptions2(src, dest) }

  // hadoop 3 API
  private def distCpOptions3(src: Path, dest: Path): DistCpOptions = {
    val clas = Class.forName("org.apache.hadoop.tools.DistCpOptions.Builder")
    val constructor = clas.getConstructor(classOf[java.util.List[Path]], classOf[Path])
    val builder = constructor.newInstance(Collections.singletonList(src), dest)
    clas.getMethod("withAppend", classOf[Boolean]).invoke(builder, java.lang.Boolean.FALSE)
    clas.getMethod("withOverwrite", classOf[Boolean]).invoke(builder, java.lang.Boolean.TRUE)
    clas.getMethod("withCopyStrategy", classOf[String]).invoke(builder, "dynamic")
    clas.getMethod("build").invoke(builder).asInstanceOf[DistCpOptions]
  }

  // hadoop 2 API
  private def distCpOptions2(src: Path, dest: Path): DistCpOptions = {
    val constructor = classOf[DistCpOptions].getConstructor(classOf[java.util.List[Path]], classOf[Path])
    val opts = constructor.newInstance(Collections.singletonList(src), dest)
    classOf[DistCpOptions].getMethod("setAppend", classOf[Boolean]).invoke(opts, java.lang.Boolean.FALSE)
    classOf[DistCpOptions].getMethod("setOverwrite", classOf[Boolean]).invoke(opts, java.lang.Boolean.TRUE)
    classOf[DistCpOptions].getMethod("setCopyStrategy", classOf[String]).invoke(opts, "dynamic")
    opts
  }

}
