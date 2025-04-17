/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.avro.io

import org.apache.avro.file.DataFileStream
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.features.avro.serialization.SimpleFeatureDatumReader
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.CloseWithLogging

import java.io.InputStream

class AvroDataFileReader(is: InputStream) extends CloseableIterator[SimpleFeature] {

  private val reader = new SimpleFeatureDatumReader()
  private val dfs = new DataFileStream[SimpleFeature](is, reader)

  if (!AvroDataFile.canParse(dfs)) {
    CloseWithLogging(dfs)
    throw new IllegalArgumentException(s"Only version ${AvroDataFile.Version} data files supported")
  }

  private val sft = AvroDataFile.getSft(dfs)

  reader.setFeatureType(sft)

  def getSft: SimpleFeatureType = sft

  override def hasNext: Boolean = dfs.hasNext

  override def next(): SimpleFeature = dfs.next()

  override def close(): Unit = dfs.close()
}
