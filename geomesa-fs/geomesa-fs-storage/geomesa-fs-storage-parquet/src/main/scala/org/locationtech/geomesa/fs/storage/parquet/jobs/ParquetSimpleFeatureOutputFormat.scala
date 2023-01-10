/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
<<<<<<< HEAD
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 1463162d60 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9f430502b2 (GEOMESA-3254 Add Bloop build support)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.parquet.jobs

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.output.{FileOutputCommitter, FileOutputFormat}
import org.apache.hadoop.mapreduce.{JobContext, OutputCommitter, TaskAttemptContext}
import org.apache.parquet.hadoop.util.ContextUtil
import org.apache.parquet.hadoop.{ParquetOutputCommitter, ParquetOutputFormat}
import org.geotools.api.feature.simple.SimpleFeature
import org.locationtech.geomesa.fs.storage.parquet.ParquetFileSystemStorage
import org.locationtech.geomesa.fs.storage.parquet.jobs.ParquetSimpleFeatureOutputFormat.ParquetMultiFileOutputCommitter

import java.io.IOException
import scala.collection.mutable

class ParquetSimpleFeatureOutputFormat extends ParquetOutputFormat[SimpleFeature] {

  private var committer: OutputCommitter = _

  // overridden to write metadata for each file instead of single file parquet expects
  override def getOutputCommitter(context: TaskAttemptContext): OutputCommitter = {
    if (committer == null) {
      val output = FileOutputFormat.getOutputPath(context)
      val extension = s".${ParquetFileSystemStorage.FileExtension}"
      committer = new ParquetMultiFileOutputCommitter(extension, output, context)
    }
    committer
  }
}

object ParquetSimpleFeatureOutputFormat {

  class ParquetMultiFileOutputCommitter(extension: String, outputPath: Path, context: TaskAttemptContext)
      extends FileOutputCommitter(outputPath, context) with LazyLogging {
    // based on parquetOutputCommitter, but for multiple output files
    @throws[IOException]
    override def commitJob(jobContext: JobContext) {
      super.commitJob(jobContext)
      val conf = ContextUtil.getConfiguration(jobContext)
      listFiles(outputPath, conf, extension).map(_.getParent).distinct.foreach { path =>
        ParquetOutputCommitter.writeMetaDataFile(conf, path)
        logger.info(s"Wrote metadata file for path $path")
      }
    }
  }

  private def listFiles(path: Path, conf: Configuration, suffix: String): Seq[Path] = {
    val fs = path.getFileSystem(conf)
    val listing = fs.listFiles(path, true)

    val result = mutable.ListBuffer.empty[Path]
    while (listing.hasNext) {
      val next = listing.next()
      if (next.isFile) {
        val p = next.getPath
        if (p.getName.endsWith(suffix)) {
          result += p
        }
      }
    }

    result.toSeq
  }
}
