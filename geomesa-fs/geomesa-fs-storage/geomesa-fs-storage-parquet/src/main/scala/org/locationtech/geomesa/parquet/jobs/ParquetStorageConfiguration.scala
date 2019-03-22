/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.parquet.jobs

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.{ParquetInputFormat, ParquetOutputFormat}
import org.locationtech.geomesa.fs.storage.common.jobs.StorageConfiguration
import org.opengis.feature.simple.SimpleFeatureType

trait ParquetStorageConfiguration extends StorageConfiguration with LazyLogging {
  override def configureOutput(sft: SimpleFeatureType, job: Job): Unit = {
    job.setOutputFormatClass(classOf[ParquetPartitionOutputFormat])
    StorageConfiguration.setSft(job.getConfiguration, sft)

    ParquetInputFormat.setReadSupportClass(job, classOf[SimpleFeatureReadSupport])
    ParquetOutputFormat.setWriteSupportClass(job, classOf[SimpleFeatureWriteSupport])

    // Parquet Options
    val summaryLevel = Option(sft.getUserData.get(ParquetOutputFormat.JOB_SUMMARY_LEVEL).asInstanceOf[String])
        .getOrElse(ParquetOutputFormat.JobSummaryLevel.NONE.toString)
    job.getConfiguration.set(ParquetOutputFormat.JOB_SUMMARY_LEVEL, summaryLevel)
    logger.debug(s"Parquet metadata summary level is $summaryLevel")

    val compression = Option(sft.getUserData.get(ParquetOutputFormat.COMPRESSION).asInstanceOf[String])
        .map(CompressionCodecName.valueOf)
        .getOrElse(CompressionCodecName.SNAPPY)
    ParquetOutputFormat.setCompression(job, compression)
    logger.debug(s"Parquet compression is $compression")
  }
}
