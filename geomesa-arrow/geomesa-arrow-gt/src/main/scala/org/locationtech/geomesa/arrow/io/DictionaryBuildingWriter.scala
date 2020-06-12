/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.io

import java.io.{Closeable, OutputStream}
import java.nio.channels.Channels
import java.util.Collections

import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.dictionary.Dictionary
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.arrow.vector.ipc.message.IpcOption
import org.apache.arrow.vector.types.pojo.{ArrowType, DictionaryEncoding, Field}
import org.locationtech.geomesa.arrow.ArrowAllocator
import org.locationtech.geomesa.arrow.vector.ArrowAttributeWriter
import org.locationtech.geomesa.arrow.vector.ArrowAttributeWriter.ArrowDictionaryWriter
import org.locationtech.geomesa.arrow.vector.ArrowDictionary.ArrowDictionaryBuilder
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.{CloseWithLogging, WithClose}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
 * Writes an arrow file of simple features. Dictionaries will be built up as features are observed.
 * Dictionaries are encoded based on `maxSize`. Values will not be correctly encoded if
 * more than `maxSize` distinct values are encountered
 *
 * @param sft simple feature type
 * @param dictionaries dictionary fields
 * @param encoding vector encoding
 * @param maxSize max size for dictionaries, will be used to pick the encoding for dictionary values
 */
class DictionaryBuildingWriter(
    sft: SimpleFeatureType,
    dictionaries: Seq[String],
    encoding: SimpleFeatureEncoding,
    ipcOpts: IpcOption,
    maxSize: Int = Short.MaxValue
  ) extends Closeable {

  private val allocator = ArrowAllocator("dictionary-builder")

  private val underlying = {
    val struct = StructVector.empty(sft.getTypeName, allocator)
    struct.allocateNew()
    struct
  }

  private val arrowWriter = underlying.getWriter

  private val idWriter = ArrowAttributeWriter.id(sft, encoding, underlying)
  private val attributeWriters = DictionaryBuildingWriter.writers(sft, underlying, dictionaries, encoding, maxSize)

  private val root = {
    val fields = Collections.singletonList[Field](underlying.getField)
    new VectorSchemaRoot(fields, Collections.singletonList[FieldVector](underlying), 0)
  }

  private var index = 0

  def size: Int = index

  def add(feature: SimpleFeature): Unit = {
    arrowWriter.setPosition(index)
    arrowWriter.start()
    idWriter.apply(index, feature)
    var i = 0
    while (i < attributeWriters.length) {
      attributeWriters(i).apply(index, feature.getAttribute(i))
      i += 1
    }
    arrowWriter.end()
    index += 1
  }

  /**
    * Clear any simple features currently stored in the vector
    */
  def clear(): Unit = {
    underlying.setValueCount(0)
    index = 0
    attributeWriters.foreach {
      case w: ArrowDictionaryWriter => w.dictionary.asInstanceOf[ArrowDictionaryBuilder].clear()
      case _ => // no-op
    }
  }

  /**
    * Writes out as a stand-alone streaming arrow file, including current dictionary values
    *
    * @param os output stream to write to
    */
  def encode(os: OutputStream): Unit = {
    arrowWriter.setValueCount(index)
    idWriter.setValueCount(index)
    attributeWriters.foreach(_.setValueCount(index))
    root.setRowCount(index)

    val dictionaries = attributeWriters.collect { case w: ArrowDictionaryWriter =>
      val name = s"dict-${w.dictionary.encoding.getId}"
      val descriptor = SimpleFeatureTypes.renameDescriptor(sft.getDescriptor(w.vector.getField.getName), name)
      val writer = ArrowAttributeWriter(sft, descriptor, None, encoding, allocator)

      var i = 0
      w.dictionary.foreach { value =>
        writer.apply(i, value)
        i += 1
      }
      writer.setValueCount(w.dictionary.length)

      new Dictionary(writer.vector, w.dictionary.encoding)
    }

    val provider = new MapDictionaryProvider(dictionaries: _*)
    WithClose(new ArrowStreamWriter(root, provider, Channels.newChannel(os), ipcOpts)) { writer =>
      writer.start()
      writer.writeBatch()
    }

    dictionaries.foreach(d => CloseWithLogging(d.getVector))
  }

  override def close(): Unit = CloseWithLogging.raise(Seq(arrowWriter, underlying, allocator))
}

object DictionaryBuildingWriter {

  import scala.collection.JavaConverters._

  /**
    * Gets an attribute writer or a dictionary building writer, as appropriate
    */
  private def writers(
      sft: SimpleFeatureType,
      vector: StructVector,
      dictionaries: Seq[String],
      encoding: SimpleFeatureEncoding,
      maxSize: Int): Array[ArrowAttributeWriter] = {
    var dictionaryId = -1L
    val writers = sft.getAttributeDescriptors.asScala.map { descriptor =>
      val dictionary = dictionaries.collectFirst {
        case name if name == descriptor.getLocalName =>
          dictionaryId += 1
          val bits = if (maxSize <= Byte.MaxValue) { 8 } else if (maxSize <= Short.MaxValue) { 16 } else { 32 }
          new ArrowDictionaryBuilder(new DictionaryEncoding(dictionaryId, false, new ArrowType.Int(bits, true)))
      }
      ArrowAttributeWriter(sft, descriptor, dictionary, encoding, vector)
    }
    writers.toArray
  }
}
