/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.parquet.jobs

import com.google.common.collect.Maps
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.hadoop.api.WriteSupport.WriteContext
import org.apache.parquet.io.api.{Binary, RecordConsumer}
import org.apache.parquet.schema.MessageType
import org.locationtech.geomesa.fs.storage.common.jobs.StorageConfiguration
import org.locationtech.geomesa.parquet.{AttributeWriter, SimpleFeatureParquetSchema}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class SimpleFeatureWriteSupport extends WriteSupport[SimpleFeature] {

  private var sft: SimpleFeatureType = _
  private var messageType: MessageType = _
  private var consumer: RecordConsumer = _
  private var writers: Array[AttributeWriter] = _
  private var idIndex: java.lang.Integer = _ // put the ID at the end of the record ? Why?

  override def init(configuration: Configuration): WriteContext = {
    this.sft = StorageConfiguration.getSft(configuration)
    this.idIndex = sft.getAttributeCount
    this.writers = SimpleFeatureParquetSchema.buildAttributeWriters(sft)
    this.messageType = SimpleFeatureParquetSchema(sft)
    new WriteContext(messageType, Maps.newHashMap())
  }

  override def prepareForWrite(recordConsumer: RecordConsumer): Unit = {
    consumer = recordConsumer
  }

  override def write(record: SimpleFeature): Unit = {
    consumer.startMessage()
    writeFields(record.getAttributes)
    writeFid(record.getID)
    consumer.endMessage()
  }

  def writeFid(id: String): Unit = {
    consumer.startField(SimpleFeatureParquetSchema.FeatureIDField, idIndex)
    consumer.addBinary(Binary.fromString(id))
    consumer.endField(SimpleFeatureParquetSchema.FeatureIDField, idIndex)
  }

  private def writeFields(attributes: java.util.List[AnyRef]): Unit = {
    var i = 0
    val len = attributes.size()
    while (i < len) {
      writers(i)(consumer, attributes.get(i))
      i += 1
    }
  }
}
