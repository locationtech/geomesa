/*******************************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ******************************************************************************/

package org.locationtech.geomesa.arrow.io

import java.io.{Closeable, Flushable, OutputStream}
import java.nio.channels.Channels
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicLong

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.dictionary.Dictionary
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider
import org.apache.arrow.vector.stream.ArrowStreamWriter
import org.apache.arrow.vector.types.pojo.{ArrowType, DictionaryEncoding}
import org.apache.arrow.vector.{NullableVarCharVector, VectorSchemaRoot}
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
  * For writing simple features to an arrow file. Not thread safe.
  *
  * @param sft simple feature type
  * @param os output stream
  * @param allocator allocator
  */
class SimpleFeatureArrowFileWriter(val sft: SimpleFeatureType, os: OutputStream, allocator: BufferAllocator)
    extends Closeable with Flushable {

  import scala.collection.JavaConversions._

  // TODO dictionaries

  //  dictionaryValues.keys.foreach { attribute =>
  //    if (sft.getDescriptor(attribute).getType.getBinding != classOf[String]) {
  //      throw new NotImplementedError("Dictionaries only supported for string types")
  //    }
  //  }
  //
  //  private val allocator = new RootAllocator(Long.MaxValue)
  //  private val dictionaries = dictionaryValues.mapValues(ArrowSimpleFeatureWriter.createDictionary(_, allocator))

  private val vector = SimpleFeatureVector.create(sft, Map.empty, allocator)
  private val root = new VectorSchemaRoot(Seq(vector.underlying.getField), Seq(vector.underlying), 0)
  private val provider = new MapDictionaryProvider()
  private val writer = new ArrowStreamWriter(root, provider, Channels.newChannel(os))

  private var index = 0

  def add(sf: SimpleFeature): Unit = {
    vector.writer.set(index, sf)
    index += 1
  }

  override def flush(): Unit = {
    if (index > 0) {
      vector.writer.setValueCount(index)
      root.setRowCount(index)
      writer.writeBatch()
      // TODO is there a better way to reset the buffer?
      vector.underlying.clear()
      vector.underlying.allocateNewSafe()
      index = 0
    }
  }

  override def close(): Unit = {
    flush()
    writer.end()
    writer.close()
    root.close()
  }
}

object SimpleFeatureArrowFileWriter {

  private final val ids = new AtomicLong(0)

  private def createDictionary(values: Seq[String], allocator: BufferAllocator): Dictionary = {
    val id = ids.getAndIncrement()
    val vector = new NullableVarCharVector(s"dictionary-$id", allocator, null)
    val mutator = vector.getMutator
    var i = 0
    values.foreach { value =>
      if (value != null) {
        mutator.set(i, value.getBytes(StandardCharsets.UTF_8))
      }
      i += 1
    }
    mutator.setValueCount(i)

    val bitWidth = if (i > Short.MaxValue) { 32 } else if (i > Byte.MaxValue) { 16 } else { 8 }
    val encoding = new DictionaryEncoding(id, false, new ArrowType.Int(bitWidth, true))

    new Dictionary(vector, encoding)
  }
}