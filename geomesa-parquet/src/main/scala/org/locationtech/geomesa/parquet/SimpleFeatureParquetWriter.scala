package org.locationtech.geomesa.parquet

import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetWriter
import org.opengis.feature.simple.SimpleFeature

/**
  * Created by afox on 5/26/17.
  */
class SimpleFeatureParquetWriter(path: Path, writeSupport: SimpleFeatureWriteSupport)
  extends ParquetWriter[SimpleFeature](path, writeSupport) {


}
