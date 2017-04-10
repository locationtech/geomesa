/*******************************************************************************
* Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
******************************************************************************/

package org.locationtech.geomesa.arrow.io

import java.io.{Closeable, OutputStream}
import java.nio.charset.StandardCharsets

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.NullableMapVector
import org.apache.arrow.vector.complex.writer.SmallIntWriter
import org.apache.arrow.vector.dictionary.Dictionary
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider
import org.apache.arrow.vector.stream.ArrowStreamWriter
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.{ArrowType, DictionaryEncoding, FieldType}
import org.locationtech.geomesa.arrow.io.DictionaryBuildingWriter.ArrowAttributeDictionaryWriter
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.GeometryPrecision
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.GeometryPrecision.GeometryPrecision
import org.locationtech.geomesa.arrow.vector.{ArrowAttributeWriter, ArrowDictionary}
import org.locationtech.geomesa.features.serialization.ObjectType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class DictionaryBuildingWriter private (val sft: SimpleFeatureType,
                                        val underlying: NullableMapVector,
                                        val dictionaries: Seq[String],
                                        val precision: GeometryPrecision)
                                       (implicit allocator: BufferAllocator) extends Closeable {

  import scala.collection.JavaConversions._

  // TODO user data at feature and schema level

  private var index = 0

  private val arrowWriter = underlying.getWriter

  private val idWriter = ArrowAttributeWriter("id", Seq(ObjectType.STRING), classOf[String], underlying, None, null)
  private val attributeWriters = DictionaryBuildingWriter.attribute(sft, underlying, dictionaries, precision).toArray

  private val root = new VectorSchemaRoot(Seq(underlying.getField), Seq(underlying), 0)

  def size: Int = index

  def add(feature: SimpleFeature): Unit = {
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
    * Clear any simple features currently stored in the vector
    */
  def clear(): Unit = {
    underlying.getMutator.setValueCount(0)
    index = 0
    attributeWriters.foreach {
      case w: ArrowAttributeDictionaryWriter => w.clear()
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
    attributeWriters.foreach(_.setValueCount(index))
    root.setRowCount(index)

    val dictionaries = attributeWriters.collect { case w: ArrowAttributeDictionaryWriter =>
      val fieldType = FieldType.nullable(ArrowType.Utf8.INSTANCE)
      val vector = new NullableVarCharVector(s"dict-${w.encoding.getId}", fieldType, allocator)
      vector.allocateNew()
      var i = 0
      w.dictionary.foreach { value =>
        if (value != null) {
          vector.getMutator.set(i, value.asInstanceOf[String].getBytes(StandardCharsets.UTF_8))
        }
        i += 1
      }
      vector.getMutator.setValueCount(w.size)
      new Dictionary(vector, w.encoding)
    }

    val provider = new MapDictionaryProvider(dictionaries: _*)

    WithClose(new ArrowStreamWriter(root, provider, os)) { writer =>
      writer.start()
      writer.writeBatch()
    }

    dictionaries.foreach(_.getVector.close())
  }

  override def close(): Unit = {
    underlying.close()
    arrowWriter.close()
  }
}

object DictionaryBuildingWriter {

  import scala.collection.JavaConversions._

  def create(sft: SimpleFeatureType,
             dictionaries: Seq[String],
             precision: GeometryPrecision = GeometryPrecision.Double)
            (implicit allocator: BufferAllocator): DictionaryBuildingWriter = {
    val underlying = new NullableMapVector(sft.getTypeName, allocator, null, null)
    underlying.allocateNew()
    new DictionaryBuildingWriter(sft, underlying, dictionaries, precision)
  }

  def attribute(sft: SimpleFeatureType,
                vector: NullableMapVector,
                dictionaries: Seq[String],
                precision: GeometryPrecision = GeometryPrecision.Double)
               (implicit allocator: BufferAllocator): Seq[ArrowAttributeWriter] = {
    sft.getAttributeDescriptors.map { descriptor =>
      val name = SimpleFeatureTypes.encodeDescriptor(sft, descriptor)
      val classBinding = descriptor.getType.getBinding
      val (objectType, bindings) = ObjectType.selectType(classBinding, descriptor.getUserData)
      if (dictionaries.contains(descriptor.getLocalName)) {
        objectType match {
          case ObjectType.STRING =>
            val encoding = new DictionaryEncoding(ArrowDictionary.nextId, false, new ArrowType.Int(16, true))
            vector.addOrGet(name, new FieldType(true, MinorType.SMALLINT.getType, encoding), classOf[NullableSmallIntVector])
            new ArrowDictionaryBuildingWriter(vector.getWriter.smallInt(name), encoding)
          case _ => throw new IllegalArgumentException(s"Dictionary only supported for string type: ${bindings.head}")
        }
      } else {
        ArrowAttributeWriter(name, bindings.+:(objectType), classBinding, vector, None, precision)
      }
    }
  }

  trait ArrowAttributeDictionaryWriter extends ArrowAttributeWriter {
    def clear(): Unit
    def size: Int
    // ordered list of dictionary values encountered by this writer
    def dictionary: Seq[AnyRef]
    def encoding: DictionaryEncoding
  }

  class ArrowDictionaryBuildingWriter(writer: SmallIntWriter, override val encoding: DictionaryEncoding)
      extends ArrowAttributeDictionaryWriter {

    private val values = scala.collection.mutable.LinkedHashMap.empty[String, Short]
    private var counter: Short = 0

    override def clear(): Unit = {
      counter = 0
      values.clear()
    }

    override def size: Int = values.size

    // note: iterator will return in insert order
    override def dictionary: Seq[AnyRef] = values.keys.toSeq

    override def apply(i: Int, value: AnyRef): Unit = {
      val index = values.getOrElseUpdate(value.asInstanceOf[String],
        { val i = counter; counter = (counter + 1).toShort; i })
      writer.writeSmallInt(index)
    }
  }
}