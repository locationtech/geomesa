/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.io

import java.io.{Closeable, OutputStream}

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.NullableMapVector
import org.apache.arrow.vector.complex.writer.SmallIntWriter
import org.apache.arrow.vector.dictionary.Dictionary
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider
import org.apache.arrow.vector.stream.ArrowStreamWriter
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.{ArrowType, DictionaryEncoding, FieldType}
import org.locationtech.geomesa.arrow.TypeBindings
import org.locationtech.geomesa.arrow.io.DictionaryBuildingWriter.ArrowAttributeDictionaryBuildingWriter
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.arrow.vector.{ArrowAttributeWriter, ArrowDictionary, SimpleFeatureVector}
import org.locationtech.geomesa.features.serialization.ObjectType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
  * Writes an arrow file of simple features. Dictionaries will be built up as features are observed.
  * Dictionaries are encoded a Int(16), i.e. 2-byte shorts. Values will not be correctly encoded if
  * more than Short.MaxValue distinct values are encountered.
  */
class DictionaryBuildingWriter private (val sft: SimpleFeatureType,
                                        val underlying: NullableMapVector,
                                        val dictionaries: Seq[String],
                                        val encoding: SimpleFeatureEncoding)
                                       (implicit allocator: BufferAllocator) extends Closeable {

  import scala.collection.JavaConversions._

  private var index = 0

  private val arrowWriter = underlying.getWriter

  private val idWriter = ArrowAttributeWriter.id(underlying, encoding.fids)
  private val attributeWriters = DictionaryBuildingWriter.attribute(sft, underlying, dictionaries, encoding).toArray

  private val root = new VectorSchemaRoot(Seq(underlying.getField), Seq(underlying), 0)

  private var maxIndex = underlying.getValueCapacity - 1

  def size: Int = index

  def add(feature: SimpleFeature): Unit = {
    // noinspection LoopVariableNotUpdated
    while (index > maxIndex) {
      expand()
    }
    arrowWriter.setPosition(index)
    arrowWriter.start()
    idWriter.apply(index, feature.getID)
    var i = 0
    while (i < attributeWriters.length) {
      attributeWriters(i).apply(index, feature.getAttribute(i))
      i += 1
    }
    arrowWriter.end()
    index += 1
  }

  /**
    * double underlying vector capacity
    */
  def expand(): Unit = {
    underlying.reAlloc()
    maxIndex = underlying.getValueCapacity - 1
  }

  /**
    * Clear any simple features currently stored in the vector
    */
  def clear(): Unit = {
    underlying.getMutator.setValueCount(0)
    index = 0
    attributeWriters.foreach {
      case w: ArrowAttributeDictionaryBuildingWriter => w.clear()
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

    val container = NullableMapVector.empty("", allocator)
    container.allocateNew() // TODO might need to expand this as we add values

    val dictionaries = attributeWriters.collect { case w: ArrowAttributeDictionaryBuildingWriter =>
      val name = s"dict-${w.encoding.getId}"
      val TypeBindings(bindings, classBinding, precision) = w.dictionaryType
      val writer = ArrowAttributeWriter(name, bindings, classBinding, container, None, Map.empty, precision)
      val vector = container.getChild(name)

      var i = 0
      w.dictionary.foreach { value =>
        if (value != null) {
          writer.apply(i, value)
        }
        i += 1
      }
      writer.setValueCount(w.size)
      container.getMutator.setValueCount(w.size)

      new Dictionary(vector, w.encoding)
    }

    val provider = new MapDictionaryProvider(dictionaries: _*)

    WithClose(new ArrowStreamWriter(root, provider, os)) { writer =>
      writer.start()
      writer.writeBatch()
    }

    container.close()
  }

  override def close(): Unit = {
    underlying.close()
    arrowWriter.close()
  }
}

object DictionaryBuildingWriter {

  import scala.collection.JavaConversions._

  /**
    * Creates a new writer
    *
    * @param sft simple feature type
    * @param dictionaries attribute names to dictionary encode
    * @param encoding encoding options
    * @param allocator allocator
    * @return
    */
  def create(sft: SimpleFeatureType,
             dictionaries: Seq[String],
             encoding: SimpleFeatureEncoding = SimpleFeatureEncoding.min(false))
            (implicit allocator: BufferAllocator): DictionaryBuildingWriter = {
    val underlying = NullableMapVector.empty(sft.getTypeName, allocator)
    underlying.allocateNew()
    new DictionaryBuildingWriter(sft, underlying, dictionaries, encoding)
  }

  /**
    * Gets an attribute writer or a dictionary building writer, as appropriate
    */
  private def attribute(sft: SimpleFeatureType,
                        vector: NullableMapVector,
                        dictionaries: Seq[String],
                        encoding: SimpleFeatureEncoding)
                       (implicit allocator: BufferAllocator): Seq[ArrowAttributeWriter] = {
    sft.getAttributeDescriptors.map { descriptor =>
      val name = descriptor.getLocalName
      val metadata = Map(SimpleFeatureVector.DescriptorKey -> SimpleFeatureTypes.encodeDescriptor(sft, descriptor))
      val classBinding = descriptor.getType.getBinding
      val (objectType, bindings) = ObjectType.selectType(classBinding, descriptor.getUserData)
      if (dictionaries.contains(descriptor.getLocalName)) {
        val dictionaryEncoding = new DictionaryEncoding(ArrowDictionary.nextId, false, new ArrowType.Int(16, true))
        val fieldType = new FieldType(true, MinorType.SMALLINT.getType, dictionaryEncoding, metadata)
        vector.addOrGet(name, fieldType, classOf[NullableSmallIntVector])
        val dictionaryType = TypeBindings(bindings.+:(objectType), classBinding, encoding)
        new ArrowAttributeDictionaryBuildingWriter(vector.getWriter.smallInt(name), dictionaryEncoding, dictionaryType)
      } else {
        ArrowAttributeWriter(name, bindings.+:(objectType), classBinding, vector, None, metadata, encoding)
      }
    }
  }

  /**
    * Tracks values seen and writes dictionary encoded ints instead
    */
  class ArrowAttributeDictionaryBuildingWriter(writer: SmallIntWriter,
                                               val encoding: DictionaryEncoding,
                                               val dictionaryType: TypeBindings) extends ArrowAttributeWriter {

    // next dictionary index to use
    private var counter: Short = 0
    // values that we have seen, and their dictionary index
    private val values = scala.collection.mutable.LinkedHashMap.empty[AnyRef, Short]

    def size: Int = values.size

    // ordered list of dictionary values encountered by this writer
    // note: iterator will return in insert order
    // we need to keep it ordered so that dictionary values match up with their index
    def dictionary: Seq[AnyRef] = values.keys.toSeq

    override def apply(i: Int, value: AnyRef): Unit = {
      val index = values.getOrElseUpdate(value, { val i = counter; counter = (counter + 1).toShort; i })
      writer.writeSmallInt(index)
    }

    def clear(): Unit = {
      counter = 0
      values.clear()
    }
  }
}