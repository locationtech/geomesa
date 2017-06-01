/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.io

import java.io.{Closeable, Flushable, OutputStream}
import java.nio.channels.Channels

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.NullableMapVector
import org.apache.arrow.vector.dictionary.Dictionary
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider
import org.apache.arrow.vector.stream.ArrowStreamWriter
import org.apache.arrow.vector.types.pojo.Schema
import org.locationtech.geomesa.arrow.TypeBindings
import org.locationtech.geomesa.arrow.vector.ArrowDictionary.HasArrowDictionary
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.arrow.vector.{ArrowAttributeWriter, ArrowDictionary, SimpleFeatureVector}
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
  * @param encoding encoding options
  * @param allocator buffer allocator
  */
class SimpleFeatureArrowFileWriter(val sft: SimpleFeatureType,
                                   os: OutputStream,
                                   dictionaries: Map[String, ArrowDictionary] = Map.empty,
                                   encoding: SimpleFeatureEncoding = SimpleFeatureEncoding.min(false),
                                   sort: Option[(String, Boolean)] = None)
                                  (implicit allocator: BufferAllocator) extends Closeable with Flushable {

  import scala.collection.JavaConversions._

  private val vector = SimpleFeatureVector.create(sft, dictionaries, encoding)

  private val provider = new MapDictionaryProvider()
  // container for holding our dictionary vectors
  private val dictionaryContainer = NullableMapVector.empty("", allocator)
  dictionaryContainer.allocateNew() // TODO might need to expand container size

  // convert the dictionary values into arrow vectors
  // make sure we load dictionaries before instantiating the stream writer
  vector.writer.attributeWriters.foreach {
    case hasDictionary: HasArrowDictionary =>
      val name = s"dictionary-${hasDictionary.dictionary.id}"
      val TypeBindings(bindings, classBinding, precision) = hasDictionary.dictionaryType
      val writer = ArrowAttributeWriter(name, bindings, classBinding, dictionaryContainer, None, Map.empty, precision)
      val vector = dictionaryContainer.getChild(name)
      var i = 0
      hasDictionary.dictionary.values.foreach { value =>
        writer.apply(i, value)
        i += 1
      }
      writer.setValueCount(i)
      vector.getMutator.setValueCount(i)
      provider.put(new Dictionary(vector, hasDictionary.dictionary.encoding))

    case _ => // no-op
  }

  private val schema = {
    val metadata = sort.map { case (field, reverse) => SimpleFeatureArrowIO.getSortAsMetadata(field, reverse) }.orNull
    new Schema(Seq(vector.underlying.getField), metadata)
  }
  private val root = new VectorSchemaRoot(schema, Seq(vector.underlying), 0)
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
      vector.clear()
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
    dictionaryContainer.close()
  }
}
