/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.io

import java.io.{Closeable, Flushable, OutputStream}
import java.nio.channels.Channels

import com.typesafe.scalalogging.LazyLogging
import org.apache.arrow.vector.dictionary.{Dictionary, DictionaryProvider}
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.arrow.vector.ipc.message.IpcOption
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
  */
class SimpleFeatureArrowFileWriter private (
    vector: SimpleFeatureVector,
    provider: DictionaryProvider with Closeable,
    os: OutputStream,
    ipcOpts: IpcOption,
    sort: Option[(String, Boolean)]
  ) extends Closeable with Flushable with LazyLogging {

  private val metadata = sort.map { case (field, reverse) => getSortAsMetadata(field, reverse) }.orNull
  private val root = createRoot(vector.underlying, metadata)
  private val writer = new ArrowStreamWriter(root, provider, Channels.newChannel(os), ipcOpts)

  private var index = 0

  def sft: SimpleFeatureType = vector.sft

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
   * Writes any currently buffered features to disk. This will create an ArrowBatch
   * containing the currently buffered features. Note that if there are no features,
   * an empty record batch will be created
   */
  override def flush(): Unit = {
    vector.writer.setValueCount(index)
    root.setRowCount(index)
    writer.writeBatch()
    vector.clear()
    index = 0
  }

  /**
    * Close the writer and flush any buffered features
    */
  override def close(): Unit = {
    try {
      if (index > 0) {
        flush()
      }
      writer.end()
    } finally {
      // note: don't close the vector schema root as it closes the vector as well
      CloseWithLogging.raise(Seq(writer, provider, vector))
    }
  }
}

object SimpleFeatureArrowFileWriter {

  /**
   * For writing simple features to an arrow file.
   *
   * Uses arrow streaming format (no footer).
   *
   * @param os output stream
   * @param sft simple feature type
   * @param dictionaries map of field names to dictionary values, used for dictionary encoding fields.
   *                     All values must be provided up front.
   * @param encoding encoding options
   */
  def apply(
      os: OutputStream,
      sft: SimpleFeatureType,
      dictionaries: Map[String, ArrowDictionary],
      encoding: SimpleFeatureEncoding,
      ipcOpts: IpcOption,
      sort: Option[(String, Boolean)]): SimpleFeatureArrowFileWriter = {
    val vector = SimpleFeatureVector.create(sft, dictionaries, encoding)
    // convert the dictionary values into arrow vectors
    // make sure we load dictionaries before instantiating the stream writer
    val provider: DictionaryProvider with Closeable = new DictionaryProvider with Closeable {
      private val dictionaries = vector.dictionaries.collect { case (_, d) => d.id -> d.toDictionary(vector.encoding) }
      override def lookup(id: Long): Dictionary = dictionaries(id)
      override def close(): Unit = CloseWithLogging(dictionaries.values)
    }
    new SimpleFeatureArrowFileWriter(vector, provider, os, ipcOpts, sort)
  }

  // convert the dictionary values into arrow vectors
  def provider(
      dictionaries: Map[String, ArrowDictionary],
      encoding: SimpleFeatureEncoding): DictionaryProvider with Closeable = {
    new DictionaryProvider with Closeable {
      private val dicts = dictionaries.collect { case (_, d) => d.id -> d.toDictionary(encoding) }
      override def lookup(id: Long): Dictionary = dicts(id)
      override def close(): Unit = CloseWithLogging(dicts.values)
    }
  }
}
