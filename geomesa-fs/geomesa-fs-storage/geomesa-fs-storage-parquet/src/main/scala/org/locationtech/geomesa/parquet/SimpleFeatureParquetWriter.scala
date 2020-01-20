/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.parquet

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.{ParquetFileWriter, ParquetWriter}
import org.locationtech.geomesa.parquet.io.SimpleFeatureWriteSupport
import org.opengis.feature.simple.SimpleFeature

object SimpleFeatureParquetWriter extends LazyLogging {

  def builder(file: Path, conf: Configuration): Builder = {
    val codec = CompressionCodecName.fromConf(conf.get("parquet.compression", "SNAPPY"))
    logger.debug(s"Using Parquet Compression codec ${codec.name()}")
    new Builder(file)
      .withConf(conf)
      .withCompressionCodec(codec)
      .withDictionaryEncoding(true)
      .withDictionaryPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
      .withMaxPaddingSize(ParquetWriter.MAX_PADDING_SIZE_DEFAULT)
      .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
      .withValidation(false)
      .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
      .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
      .withRowGroupSize(8*1024*1024)
  }

  class Builder private [SimpleFeatureParquetWriter] (file: Path)
      extends ParquetWriter.Builder[SimpleFeature, Builder](file) {
    override def self(): Builder = this
    override protected def getWriteSupport(conf: Configuration): WriteSupport[SimpleFeature] =
      new SimpleFeatureWriteSupport
  }
}
