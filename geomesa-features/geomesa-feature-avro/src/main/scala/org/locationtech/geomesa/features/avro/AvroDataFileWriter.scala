/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.avro

import java.io._
import java.util.zip.Deflater

import org.apache.avro.file.{CodecFactory, DataFileWriter}
import org.geotools.data.simple.SimpleFeatureCollection
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
 * Class that writes out Avro SimpleFeature Data Files which carry the SimpleFeatureType Schema
 * along with them.
 *
 * @param os output stream.
 * @param sft simple feature type being written
 * @param compression compression level, from -1 to 9. @see java.util.zip.Deflater
 */
class AvroDataFileWriter(os: OutputStream,
                         sft: SimpleFeatureType,
                         compression: Int = Deflater.DEFAULT_COMPRESSION) extends Closeable with Flushable {

  private val schema = AvroSimpleFeatureUtils.generateSchema(sft, withUserData = true, withFeatureId = true, namespace = sft.getName.getNamespaceURI)
  private val writer = new AvroSimpleFeatureWriter(sft, SerializationOptions.withUserData)
  private val dfw    = new DataFileWriter[SimpleFeature](writer)

  if (compression != Deflater.NO_COMPRESSION) {
    dfw.setCodec(CodecFactory.deflateCodec(compression))
  }
  AvroDataFile.setMetaData(dfw, sft)
  dfw.create(schema, os)

  def append(fc: SimpleFeatureCollection): Unit =
    SelfClosingIterator(fc.features()).foreach(dfw.append)

  def append(sf: SimpleFeature): Unit = dfw.append(sf)

  override def close(): Unit = if (dfw != null) { dfw.close() }

  override def flush(): Unit = if (dfw != null) { dfw.flush() }
}
