/***********************************************************************
* Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.arrow.io

import java.io.{Closeable, Flushable, OutputStream}
import java.nio.channels.Channels
import java.nio.charset.StandardCharsets

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.dictionary.Dictionary
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider
import org.apache.arrow.vector.stream.ArrowStreamWriter
import org.apache.arrow.vector.types.pojo.{ArrowType, FieldType}
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.GeometryPrecision
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.GeometryPrecision.GeometryPrecision
import org.locationtech.geomesa.arrow.vector.{ArrowDictionary, SimpleFeatureVector}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
  * For writing simple features to an arrow file.
  *
  * Uses arrow streaming format (no footer).
  *
  * @param sft simple feature type
  * @param os output stream
  * @param dictionaries map of field names to dictionary values, used for dictionary encoding fields.
  *                     All values must be provided up front.
  * @param includeFids encode feature ids in vectors or not
  * @param precision precision of coordinates - double or float
  * @param allocator buffer allocator
  */
class SimpleFeatureArrowFileWriter(val sft: SimpleFeatureType,
                                   os: OutputStream,
                                   dictionaries: Map[String, ArrowDictionary] = Map.empty,
                                   includeFids: Boolean = true,
                                   precision: GeometryPrecision = GeometryPrecision.Double)
                                  (implicit allocator: BufferAllocator) extends Closeable with Flushable {

  import scala.collection.JavaConversions._

  private val provider = new MapDictionaryProvider()
  // convert the dictionary values into arrow vectors
  // make sure we load dictionaries before instantiating the vector
  private val dictionaryVectors = dictionaries.values.map { dictionary =>
    val vector = new NullableVarCharVector(s"dictionary-${dictionary.id}", new FieldType(true, ArrowType.Utf8.INSTANCE, null), allocator)
//    vector.setInitialCapacity(dictionary.values.length)
    vector.allocateNew()
    val mutator = vector.getMutator
    var i = 0
//    var capacity = vector.getValueCapacity
    dictionary.values.foreach { value =>
      val bytes = value.toString.getBytes(StandardCharsets.UTF_8)
      // TODO figure out capacity checks
//      while (capacity < i || vector.getByteCapacity < vector.getCurrentSizeInBytes + bytes.length) {
//        vector.reAlloc()
//        capacity = vector.getValueCapacity
//      }
      mutator.set(i, bytes)
      i += 1
    }
    mutator.setValueCount(i)
    provider.put(new Dictionary(vector, dictionary.encoding))
    vector
  }

  private val vector = SimpleFeatureVector.create(sft, dictionaries, includeFids, precision)
  private val root = new VectorSchemaRoot(Seq(vector.underlying.getField), Seq(vector.underlying), 0)
  private val writer = new ArrowStreamWriter(root, provider, Channels.newChannel(os))

  private var index = 0

  /**
    * Start writing - this will write the schema and any dictionaries. Optional operation,
    * adding a feature or closing the writer will automatically start if needed.
    */
  def start(): Unit = writer.start()

  /**
    * Buffer a feature to write
    *
    * @param sf simple feature
    */
  def add(sf: SimpleFeature): Unit = {
    vector.writer.set(index, sf)
    index += 1
  }

  /**
    * Writs any currently buffered features to disk. This will create an ArrowBatch
    * containing the currently buffered features.
    */
  override def flush(): Unit = {
    if (index > 0) {
      vector.writer.setValueCount(index)
      root.setRowCount(index)
      writer.writeBatch()
      vector.reset()
      index = 0
    }
  }

  /**
    * Close the writer and flush any buffered features
    */
  override def close(): Unit = {
    flush()
    writer.end()
    writer.close()
    root.close()
    dictionaryVectors.foreach(_.close())
  }
}
