/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.io

import java.io.{Closeable, OutputStream}

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.dictionary.Dictionary
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.{ArrowType, DictionaryEncoding, FieldType}
import org.locationtech.geomesa.arrow.TypeBindings
import org.locationtech.geomesa.arrow.io.DictionaryBuildingWriter.ArrowAttributeDictionaryBuildingWriter
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.arrow.vector.{ArrowAttributeWriter, SimpleFeatureVector}
import org.locationtech.geomesa.features.serialization.ObjectType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.{CloseWithLogging, WithClose}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
  * Writes an arrow file of simple features. Dictionaries will be built up as features are observed.
  * Dictionaries are encoded a Int(16), i.e. 2-byte shorts. Values will not be correctly encoded if
  * more than Short.MaxValue distinct values are encountered.
  */
class DictionaryBuildingWriter private (val sft: SimpleFeatureType,
                                        val underlying: StructVector,
                                        val dictionaries: Seq[String],
                                        val encoding: SimpleFeatureEncoding,
                                        val maxSize: Int)
                                       (implicit allocator: BufferAllocator) extends Closeable {

  import scala.collection.JavaConversions._

  private var index = 0

  private val arrowWriter = underlying.getWriter

  private val idWriter = ArrowAttributeWriter.id(sft, Some(underlying), encoding)
  private val attributeWriters =
    DictionaryBuildingWriter.attribute(sft, underlying, dictionaries, encoding, maxSize).toArray

  private val root = new VectorSchemaRoot(Seq(underlying.getField), Seq(underlying), 0)

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
      case w: ArrowAttributeDictionaryBuildingWriter[_] => w.clear()
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

    val dictionaries = attributeWriters.collect { case w: ArrowAttributeDictionaryBuildingWriter[_] =>
      val name = s"dict-${w.encoding.getId}"
      val TypeBindings(bindings, precision) = w.dictionaryType
      val writer = ArrowAttributeWriter(name, bindings, None, None, Map.empty, precision)

      var i = 0
      w.dictionary.foreach { value =>
        writer.apply(i, value)
        i += 1
      }
      writer.setValueCount(w.size)

      new Dictionary(writer.vector, w.encoding)
    }

    WithClose(new ArrowStreamWriter(root, new MapDictionaryProvider(dictionaries: _*), os)) { writer =>
      writer.start()
      writer.writeBatch()
    }

    dictionaries.foreach(d => CloseWithLogging(d.getVector))
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
             encoding: SimpleFeatureEncoding = SimpleFeatureEncoding.Min,
             maxSize: Int = Short.MaxValue)
            (implicit allocator: BufferAllocator): DictionaryBuildingWriter = {
    val underlying = StructVector.empty(sft.getTypeName, allocator)
    underlying.allocateNew()
    new DictionaryBuildingWriter(sft, underlying, dictionaries, encoding, maxSize)
  }

  /**
    * Gets an attribute writer or a dictionary building writer, as appropriate
    */
  private def attribute(sft: SimpleFeatureType,
                        vector: StructVector,
                        dictionaries: Seq[String],
                        encoding: SimpleFeatureEncoding,
                        maxSize: Int)
                       (implicit allocator: BufferAllocator): Seq[ArrowAttributeWriter] = {
    var dictionaryId = -1L
    sft.getAttributeDescriptors.map { descriptor =>
      val name = descriptor.getLocalName
      val metadata = Map(SimpleFeatureVector.DescriptorKey -> SimpleFeatureTypes.encodeDescriptor(sft, descriptor))
      val classBinding = descriptor.getType.getBinding
      val bindings = ObjectType.selectType(classBinding, descriptor.getUserData)
      if (dictionaries.contains(name)) {
        dictionaryId += 1
        val dictionaryType = TypeBindings(bindings, encoding)
        if (maxSize <= Byte.MaxValue) {
          val dictionaryEncoding = new DictionaryEncoding(dictionaryId, false, new ArrowType.Int(8, true))
          val fieldType = new FieldType(true, MinorType.TINYINT.getType, dictionaryEncoding, metadata)
          val child = vector.addOrGet(name, fieldType, classOf[TinyIntVector])
          new ArrowAttributeByteDictionaryBuildingWriter(child, dictionaryEncoding, dictionaryType)
        } else if (maxSize <= Short.MaxValue) {
          val dictionaryEncoding = new DictionaryEncoding(dictionaryId, false, new ArrowType.Int(16, true))
          val fieldType = new FieldType(true, MinorType.SMALLINT.getType, dictionaryEncoding, metadata)
          val child = vector.addOrGet(name, fieldType, classOf[SmallIntVector])
          new ArrowAttributeShortDictionaryBuildingWriter(child, dictionaryEncoding, dictionaryType)
        } else if (maxSize <= Int.MaxValue) {
          val dictionaryEncoding = new DictionaryEncoding(dictionaryId, false, new ArrowType.Int(32, true))
          val fieldType = new FieldType(true, MinorType.INT.getType, dictionaryEncoding, metadata)
          val child = vector.addOrGet(name, fieldType, classOf[IntVector])
          new ArrowAttributeIntDictionaryBuildingWriter(child, dictionaryEncoding, dictionaryType)
        } else {
          throw new IllegalArgumentException(s"MaxSize must be less than or equal to Int.MaxValue (${Int.MaxValue})")
        }
      } else {
        ArrowAttributeWriter(name, bindings, Some(vector), None, metadata, encoding)
      }
    }
  }

  /**
    * Tracks values seen and writes dictionary encoded ints instead
    */
  abstract class ArrowAttributeDictionaryBuildingWriter[T](val encoding: DictionaryEncoding,
                                                           val dictionaryType: TypeBindings)
      extends ArrowAttributeWriter {

    // next dictionary index to use
    protected var counter: Int = 0
    // values that we have seen, and their dictionary index
    protected val values: scala.collection.mutable.LinkedHashMap[AnyRef, T] =
      scala.collection.mutable.LinkedHashMap.empty[AnyRef, T]

    def size: Int = values.size

    // ordered list of dictionary values encountered by this writer
    // note: iterator will return in insert order
    // we need to keep it ordered so that dictionary values match up with their index
    def dictionary: Seq[AnyRef] = values.keys.toSeq

    def clear(): Unit = {
      counter = 0
      values.clear()
    }
  }

  class ArrowAttributeByteDictionaryBuildingWriter(override val vector: TinyIntVector,
                                                   encoding: DictionaryEncoding,
                                                   dictionaryType: TypeBindings)
      extends ArrowAttributeDictionaryBuildingWriter[Byte](encoding, dictionaryType) {
    override def apply(i: Int, value: AnyRef): Unit = {
      val index = values.getOrElseUpdate(value, { val i = counter.toByte; counter = counter + 1; i })
      vector.setSafe(i, index)
    }
  }

  class ArrowAttributeShortDictionaryBuildingWriter(override val vector: SmallIntVector,
                                                    encoding: DictionaryEncoding,
                                                    dictionaryType: TypeBindings)
      extends ArrowAttributeDictionaryBuildingWriter[Short](encoding, dictionaryType) {
    override def apply(i: Int, value: AnyRef): Unit = {
      val index = values.getOrElseUpdate(value, { val i = counter.toShort; counter = counter + 1; i })
      vector.setSafe(i, index)
    }
  }

  class ArrowAttributeIntDictionaryBuildingWriter(override val vector: IntVector,
                                                  encoding: DictionaryEncoding,
                                                  dictionaryType: TypeBindings)
      extends ArrowAttributeDictionaryBuildingWriter[Int](encoding, dictionaryType) {
    override def apply(i: Int, value: AnyRef): Unit = {
      val index = values.getOrElseUpdate(value, { val i = counter; counter = counter + 1; i })
      vector.setSafe(i, index)
    }
  }
}