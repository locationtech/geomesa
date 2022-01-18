/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.ingest

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.tools.{DistCp, DistCpOptions}
import org.locationtech.geomesa.jobs.JobResult.JobSuccess
import org.locationtech.geomesa.jobs.{JobResult, StatusCallback}
import org.locationtech.geomesa.tools.Command
import org.locationtech.geomesa.tools.utils.JobRunner

import java.util.Collections

object StorageJobUtils extends LazyLogging {

  def distCopy(srcRoot: Path, destRoot: Path, statusCallback: StatusCallback): JobResult = {
    statusCallback.reset()

    Command.user.info("Submitting job 'DistCp' - please wait...")

    val opts = distCpOptions(srcRoot, destRoot)
    val job = new DistCp(new Configuration, opts).execute()

    Command.user.info(s"Tracking available at ${job.getStatus.getTrackingUrl}")

    JobRunner.monitor(job, statusCallback, Seq.empty, Seq.empty).merge {
      Some(JobSuccess(s"Successfully copied data to $destRoot", Map.empty))
    }
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
