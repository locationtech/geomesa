/***********************************************************************
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.avro
package io

import org.apache.avro.file.{CodecFactory, DataFileWriter}
import org.geotools.data.simple.SimpleFeatureCollection
import org.locationtech.geomesa.features.SerializationOption.{SerializationOption, SerializationOptions}
import org.locationtech.geomesa.features.avro.serialization.SimpleFeatureDatumWriter
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import java.io.{Closeable, Flushable, OutputStream}
import java.util.zip.Deflater

/**
 * Class that writes out Avro SimpleFeature Data Files which carry the SimpleFeatureType Schema
 * along with them.
 *
 * @param os output stream.
 * @param sft simple feature type being written
 * @param compression compression level, from -1 to 9. @see java.util.zip.Deflater
 */
class AvroDataFileWriter(
    os: OutputStream,
    sft: SimpleFeatureType,
    compression: Int = Deflater.DEFAULT_COMPRESSION,
    opts: Set[SerializationOption] = Set.empty
  ) extends Closeable with Flushable {

  // constructors for java interop
  def this(os: OutputStream, sft: SimpleFeatureType, compression: Int) = this(os, sft, compression, Set.empty)

  private val writer = new SimpleFeatureDatumWriter(sft, SerializationOptions.withUserData ++ opts)
  private val dfw    = new DataFileWriter[SimpleFeature](writer)

  if (compression != Deflater.NO_COMPRESSION) {
    dfw.setCodec(CodecFactory.deflateCodec(compression))
  }
  AvroDataFile.setMetaData(dfw, sft)
  dfw.create(writer.getSchema, os)

  def append(fc: SimpleFeatureCollection): Unit =
    SelfClosingIterator(fc.features()).foreach(dfw.append)

  def append(sf: SimpleFeature): Unit = dfw.append(sf)

  override def close(): Unit = if (dfw != null) { dfw.close() }

  override def flush(): Unit = if (dfw != null) { dfw.flush() }
}
