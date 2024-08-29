/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.tools.{DistCp, DistCpOptions}
import org.locationtech.geomesa.jobs.JobResult.JobSuccess
import org.locationtech.geomesa.jobs.{JobResult, StatusCallback}
import org.locationtech.geomesa.tools.Command

/**
 * Executes a hadoop distcp
 *
 * @param conf configuration
 */
class DistributedCopy(conf: Configuration = new Configuration()) {

  /**
   * Execute the job
   *
   * @param sourceFileList file containing list of sources to copy
   * @param dest destination
   * @param statusCallback status callback
   * @return
   */
  def copy(sourceFileList: Path, dest: Path, statusCallback: StatusCallback): JobResult =
    copy(DistributedCopy.distCpOptions(Right(sourceFileList), dest), statusCallback)

  /**
   * Execute the job
   *
   * @param sources source files to copy
   * @param dest destination
   * @param statusCallback status callback
   * @return
   */
  def copy(sources: Seq[Path], dest: Path, statusCallback: StatusCallback): JobResult =
    copy(DistributedCopy.distCpOptions(Left(sources), dest), statusCallback)

  /**
   * Executes the job
   *
   * @param opts opts
   * @param statusCallback status callback
   * @return
   */
  private def copy(opts: DistCpOptions, statusCallback: StatusCallback): JobResult = {
    Command.user.info("Submitting job 'DistCp' - please wait...")
    statusCallback.reset()
    val job = new DistCp(conf, opts).execute()

    Command.user.info(s"Tracking available at ${job.getStatus.getTrackingUrl}")

    JobRunner.monitor(job, statusCallback, Seq.empty, Seq.empty).merge {
      Some(JobSuccess(s"Successfully copied data to ${opts.getTargetPath}", Map.empty))
    }
  }
}

object DistributedCopy {

  import scala.collection.JavaConverters._

  private def distCpOptions(sources: Either[Seq[Path], Path], dest: Path): DistCpOptions =
    try { distCpOptions3(sources, dest) } catch { case _: ClassNotFoundException => distCpOptions2(sources, dest) }

  // hadoop 3 API
  private def distCpOptions3(sources: Either[Seq[Path], Path], dest: Path): DistCpOptions = {
    val builder = sources match {
      case Right(file) => new DistCpOptions.Builder(file, dest)
      case Left(dirs)  => new DistCpOptions.Builder(dirs.asJava, dest)
    }
    builder.withAppend(false).withOverwrite(true).withBlocking(false).withCopyStrategy("dynamic").build()
  }

  // hadoop 2 API
  private def distCpOptions2(sources: Either[Seq[Path], Path], dest: Path): DistCpOptions = {
    val opts = sources match {
      case Right(file) =>
        classOf[DistCpOptions].getConstructor(classOf[Path], classOf[Path]).newInstance(file, dest)
      case Left(dirs) =>
        classOf[DistCpOptions].getConstructor(classOf[java.util.List[Path]], classOf[Path]).newInstance(dirs.asJava, dest)
    }
    classOf[DistCpOptions].getMethod("setAppend", classOf[Boolean]).invoke(opts, java.lang.Boolean.FALSE)
    classOf[DistCpOptions].getMethod("setOverwrite", classOf[Boolean]).invoke(opts, java.lang.Boolean.TRUE)
    classOf[DistCpOptions].getMethod("setCopyStrategy", classOf[String]).invoke(opts, "dynamic")
    opts
  }
}
