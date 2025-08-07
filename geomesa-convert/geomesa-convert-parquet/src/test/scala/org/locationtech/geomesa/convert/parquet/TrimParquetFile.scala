/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.convert.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.example.{ExampleParquetWriter, GroupReadSupport}
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetFileWriter, ParquetReader}
import org.locationtech.geomesa.utils.io.WithClose

object TrimParquetFile {
  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    if (args == null || args.lengthCompare(2) < 0) {
      throw new IllegalArgumentException("Usage: <src> <dest>")
    }
    val source = new Path(args(0))
    val dest = new Path(args(1))

    val footer = ParquetFileReader.readFooter(conf, source, ParquetMetadataConverter.NO_FILTER)
    WithClose(ParquetReader.builder(new GroupReadSupport(), source).build()) { reader =>
      val builder =
        ExampleParquetWriter.builder(dest)
          .withType(footer.getFileMetaData.getSchema)
          .withExtraMetaData(footer.getFileMetaData.getKeyValueMetaData)
          .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
      WithClose(builder.build()) { writer =>
        var group = reader.read()
        var i = 0
        while (group != null && i < 10) {
          writer.write(group)
          i += 1
          group = reader.read()
        }
      }
    }
  }
}
