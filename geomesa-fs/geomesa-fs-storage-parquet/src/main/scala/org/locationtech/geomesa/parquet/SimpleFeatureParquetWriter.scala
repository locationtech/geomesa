/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.{ParquetFileWriter, ParquetWriter}
import org.opengis.feature.simple.SimpleFeature

class SimpleFeatureParquetWriter(path: Path, conf: Configuration)
  extends ParquetWriter[SimpleFeature](
    path,
    ParquetFileWriter.Mode.OVERWRITE,
    new SimpleFeatureWriteSupport,
    CompressionCodecName.SNAPPY,
    ParquetWriter.DEFAULT_BLOCK_SIZE,
    ParquetWriter.DEFAULT_PAGE_SIZE,
    ParquetWriter.DEFAULT_PAGE_SIZE,
    true,
    false,
    ParquetProperties.WriterVersion.PARQUET_2_0,
    conf
  ) { }
