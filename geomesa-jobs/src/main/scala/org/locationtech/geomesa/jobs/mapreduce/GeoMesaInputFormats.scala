/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.jobs.mapreduce

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce._
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

object GeoMesaInputFormats {
}

/**
  * Record reader that delegates to accumulo record readers and transforms the key/values coming back into
  * simple features.
  *
  * @param reader
  */
class GeoMesaRecordReader[FI <: GeoMesaFeatureIndex[_, _, _]]
(
  sft: SimpleFeatureType,
  table: FI,
  reader: RecordReader[Array[Byte], Array[Byte]],
  hasId: Boolean,
  decoder: org.locationtech.geomesa.features.SimpleFeatureSerializer
) extends RecordReader[Text, SimpleFeature] {

  private var currentFeature: SimpleFeature = null

  private val getId = table.getIdFromRow(sft)

  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
    reader.initialize(split, context)
  }
  override def getProgress: Float = reader.getProgress

  override def nextKeyValue(): Boolean = nextKeyValueInternal()

  /**
    * Get the next key value from the underlying reader, incrementing the reader when required
    */
  private def nextKeyValueInternal(): Boolean = {
    if (reader.nextKeyValue()) {
      currentFeature = decoder.deserialize(reader.getCurrentValue)
      if (!hasId) {
        val row = reader.getCurrentKey
        currentFeature.getIdentifier.asInstanceOf[FeatureIdImpl].setID(getId(row, 0, row.length))
      }
      true
    } else {
      false
    }
  }

  override def getCurrentValue: SimpleFeature = currentFeature

  override def getCurrentKey = new Text(currentFeature.getID)

  override def close(): Unit = { reader.close() }
}

