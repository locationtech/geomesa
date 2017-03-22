/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.avro

import java.io.{Closeable, InputStream}

import org.apache.avro.file.DataFileStream
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.opengis.feature.simple.SimpleFeature

class AvroDataFileReader(is: InputStream) extends Iterator[SimpleFeature] with Closeable {

  private val datumReader = new FeatureSpecificReader(null, null, SerializationOptions.withUserData)
  private val dfs = new DataFileStream[AvroSimpleFeature](is, datumReader)

  if (!AvroDataFile.canParse(dfs)) {
    throw new IllegalArgumentException(s"Only version ${AvroDataFile.Version} data files supported")
  }

  private val sft = AvroDataFile.getSft(dfs)
  private val schema = dfs.getSchema

  datumReader.setSchema(schema)
  datumReader.setTypes(sft, sft)

  def getSft = sft

  override def hasNext: Boolean = dfs.hasNext

  override def next(): SimpleFeature = dfs.next()

  override def close(): Unit = dfs.close()
}