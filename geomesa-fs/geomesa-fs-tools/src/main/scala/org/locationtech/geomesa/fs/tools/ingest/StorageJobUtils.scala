/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
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
import org.locationtech.geomesa.tools.ingest.AbstractConverterIngest.StatusCallback

object StorageJobUtils extends LazyLogging {

  def distCopy(srcRoot: Path, destRoot: Path, statusCallback: StatusCallback): Boolean = {
    statusCallback.reset()

    Command.user.info("Submitting DistCp job - please wait...")
    val opts = new DistCpOptions(Collections.singletonList(srcRoot), destRoot)
    opts.setAppend(false)
    opts.setOverwrite(true)
    opts.setCopyStrategy("dynamic")
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
}
