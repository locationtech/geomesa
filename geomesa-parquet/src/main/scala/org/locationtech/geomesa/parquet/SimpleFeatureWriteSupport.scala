package org.locationtech.geomesa.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.io.api.RecordConsumer
import org.opengis.feature.simple.SimpleFeature

/**
  * Created by afox on 5/26/17.
  */
class SimpleFeatureWriteSupport extends WriteSupport[SimpleFeature] {

  override def init(configuration: Configuration): WriteSupport.WriteContext = ???

  override def prepareForWrite(recordConsumer: RecordConsumer): Unit = ???

  override def write(record: SimpleFeature): Unit = ???
}
