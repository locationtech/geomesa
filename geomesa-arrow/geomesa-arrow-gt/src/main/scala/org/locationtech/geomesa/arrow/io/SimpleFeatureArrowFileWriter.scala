/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.io

import java.io.{Closeable, Flushable, OutputStream}
import java.nio.channels.Channels

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.arrow.vector.{ArrowDictionary, SimpleFeatureVector}
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
  * For writing simple features to an arrow file.
  *
  * Uses arrow streaming format (no footer).
  *
  * @param vector simple feature vector
  * @param provider dictionary provider
  * @param os output stream
  * @param allocator buffer allocator
  */
class SimpleFeatureArrowFileWriter private (vector: SimpleFeatureVector,
                                            provider: MapDictionaryProvider,
                                            os: OutputStream,
                                            sort: Option[(String, Boolean)])
                                           (implicit allocator: BufferAllocator) extends Closeable with Flushable {

  private val metadata = sort.map { case (field, reverse) => SimpleFeatureArrowIO.getSortAsMetadata(field, reverse) }.orNull
  private val root = SimpleFeatureArrowIO.createRoot(vector.underlying, metadata)
  private val writer = new ArrowStreamWriter(root, provider, Channels.newChannel(os))

  private var index = 0

  def sft: SimpleFeatureType = vector.sft

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
    import scala.collection.JavaConversions._

    flush()
    writer.end()
    CloseWithLogging(writer)
    CloseWithLogging(root)
    provider.getDictionaryIds.foreach(id => CloseWithLogging(provider.lookup(id).getVector))
  }
}

object SimpleFeatureArrowFileWriter {

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
  def apply(sft: SimpleFeatureType,
            os: OutputStream,
            dictionaries: Map[String, ArrowDictionary] = Map.empty,
            encoding: SimpleFeatureEncoding = SimpleFeatureEncoding.Min,
            sort: Option[(String, Boolean)] = None)
           (implicit allocator: BufferAllocator): SimpleFeatureArrowFileWriter = {
    apply(SimpleFeatureVector.create(sft, dictionaries, encoding), os, sort)
  }

  def apply(vector: SimpleFeatureVector,
            os: OutputStream,
            sort: Option[(String, Boolean)])
           (implicit allocator: BufferAllocator): SimpleFeatureArrowFileWriter = {
    // convert the dictionary values into arrow vectors
    // make sure we load dictionaries before instantiating the stream writer
    val provider = new MapDictionaryProvider(vector.dictionaries.values.map(_.toDictionary(vector.encoding)).toSeq: _*)
    new SimpleFeatureArrowFileWriter(vector, provider, os, sort)
  }
}