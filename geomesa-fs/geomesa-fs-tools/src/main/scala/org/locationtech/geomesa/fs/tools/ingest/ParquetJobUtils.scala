/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.ingest

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.hadoop.mapreduce.JobStatus
import org.apache.hadoop.tools.{DistCp, DistCpOptions}
import org.locationtech.geomesa.parquet.SimpleFeatureReadSupport
import org.locationtech.geomesa.tools.Command
import org.locationtech.geomesa.tools.ingest.AbstractIngest.StatusCallback
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._
import scala.collection.mutable

object ParquetJobUtils extends LazyLogging {

  def distCopy(srcRoot: Path, dest: Path, sft: SimpleFeatureType, conf: Configuration, statusCallback: StatusCallback): Boolean = {
    val typeName = sft.getTypeName
    val typePath = new Path(srcRoot, typeName)
    val destTypePath = new Path(dest, typeName)

    statusCallback.reset()

    Command.user.info("Submitting distcp job - please wait...")
    val opts = new DistCpOptions(List(typePath), destTypePath)
    opts.setAppend(false)
    opts.setOverwrite(true)
    opts.setCopyStrategy("dynamic")
    val job = new DistCp(new Configuration, opts).execute()

    Command.user.info(s"Tracking available at ${job.getStatus.getTrackingUrl}")

    // distCp has no reduce phase
    while (!job.isComplete) {
      if (job.getStatus.getState != JobStatus.State.PREP) {
        statusCallback("DistCp (stage 3/3): ", job.mapProgress(), Seq.empty, done = false)
      }
      Thread.sleep(1000)
    }
    statusCallback("DistCp (stage 3/3): ", job.mapProgress(), Seq.empty, done = true)

    val success = job.isSuccessful
    if (success) {
      Command.user.info(s"Successfully copied data to $dest")
    } else {
      Command.user.error(s"failed to copy data to $dest")
    }
    success
  }

  // TODO parallelize if the filesystems are not the same
  def copyData(srcRoot: Path, destRoot: Path, sft: SimpleFeatureType, conf: Configuration): Boolean = {
    val typeName = sft.getTypeName
    Command.user.info(s"Job finished...copying data from $srcRoot to $destRoot for type $typeName")

    val srcFS = srcRoot.getFileSystem(conf)
    val destFS = destRoot.getFileSystem(conf)

    val typePath = new Path(srcRoot, typeName)
    val foundFiles = srcFS.listFiles(typePath, true)

    val storageFiles = mutable.ListBuffer.empty[Path]
    while (foundFiles.hasNext) {
      val f = foundFiles.next()
      if (!f.isDirectory) {
        storageFiles += f.getPath
      }
    }

    storageFiles.forall { f =>
      val child = f.toString.replace(srcRoot.toString, "")
      val target = new Path(destRoot, if (child.startsWith("/")) child.drop(1) else child)
      logger.info(s"Moving $f to $target")
      if (!destFS.exists(target.getParent)) {
        destFS.mkdirs(target.getParent)
      }
      FileUtil.copy(srcFS, f, destFS, target, true, true, conf)
    }
  }


  //
  // Common configuration options
  //

  def setSimpleFeatureType(conf: Configuration, sft: SimpleFeatureType): Unit = {
    // Validate that there is a partition scheme
    org.locationtech.geomesa.fs.storage.common.PartitionScheme.extractFromSft(sft)
    SimpleFeatureReadSupport.setSft(sft, conf)
  }

  def getSimpleFeatureType(conf: Configuration): SimpleFeatureType = {
    SimpleFeatureReadSupport.getSft(conf)
  }

}
