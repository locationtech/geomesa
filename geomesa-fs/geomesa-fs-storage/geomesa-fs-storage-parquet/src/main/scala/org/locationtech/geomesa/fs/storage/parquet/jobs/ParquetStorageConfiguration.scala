/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 1463162d60 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9f430502b2 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> dce8c58b44 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b727e40f7c (GEOMESA-3254 Add Bloop build support)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.parquet.jobs

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.{ParquetInputFormat, ParquetOutputFormat}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.fs.storage.common.jobs.StorageConfiguration
import org.locationtech.geomesa.fs.storage.parquet.io.{SimpleFeatureReadSupport, SimpleFeatureWriteSupport}

trait ParquetStorageConfiguration extends StorageConfiguration with LazyLogging {
  override def configureOutput(sft: SimpleFeatureType, job: Job): Unit = {
    job.setOutputFormatClass(classOf[ParquetPartitionOutputFormat])
    StorageConfiguration.setSft(job.getConfiguration, sft)

    ParquetInputFormat.setReadSupportClass(job, classOf[SimpleFeatureReadSupport])
    ParquetOutputFormat.setWriteSupportClass(job, classOf[SimpleFeatureWriteSupport])

    // Parquet Options
    val summaryLevel =
      Option(job.getConfiguration.get(ParquetOutputFormat.JOB_SUMMARY_LEVEL))
          .orElse(Option(sft.getUserData.get(ParquetOutputFormat.JOB_SUMMARY_LEVEL).asInstanceOf[String]))
          .getOrElse(ParquetOutputFormat.JobSummaryLevel.NONE.toString)
    job.getConfiguration.set(ParquetOutputFormat.JOB_SUMMARY_LEVEL, summaryLevel)
    logger.debug(s"Parquet metadata summary level is $summaryLevel")

    val compression =
      Option(job.getConfiguration.get(ParquetOutputFormat.COMPRESSION))
          .orElse(Option(sft.getUserData.get(ParquetOutputFormat.COMPRESSION).asInstanceOf[String]))
          .map(CompressionCodecName.valueOf)
          .getOrElse(CompressionCodecName.SNAPPY)
    ParquetOutputFormat.setCompression(job, compression)
    logger.debug(s"Parquet compression is $compression")
  }
}
