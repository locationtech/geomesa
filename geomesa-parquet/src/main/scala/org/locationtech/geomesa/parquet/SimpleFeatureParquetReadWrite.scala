package org.locationtech.geomesa.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.{ParquetFileWriter, ParquetReader, ParquetWriter}
import org.opengis.feature.simple.SimpleFeature

class SimpleFeatureParquetWriter(path: Path, writeSupport: SimpleFeatureWriteSupport)
  extends ParquetWriter[SimpleFeature](
    path,
    ParquetFileWriter.Mode.OVERWRITE,
    writeSupport,
    CompressionCodecName.SNAPPY,
    ParquetWriter.DEFAULT_BLOCK_SIZE,
    ParquetWriter.DEFAULT_PAGE_SIZE,
    ParquetWriter.DEFAULT_PAGE_SIZE,
    true,
    false,
    ParquetProperties.WriterVersion.PARQUET_2_0,
    new Configuration
  ) {


}

class SimpleFeatureParquetReader(path: Path, readSupport: SimpleFeatureReadSupport)
  extends ParquetReader[SimpleFeature](path, readSupport) {

}

