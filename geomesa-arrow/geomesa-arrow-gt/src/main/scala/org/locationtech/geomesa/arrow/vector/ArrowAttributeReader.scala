/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.vector

import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicLong
import java.util.{Date, UUID}

import org.locationtech.jts.geom._
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.{BaseRepeatedValueVector, FixedSizeListVector, ListVector, StructVector}
import org.apache.arrow.vector.holders._
import org.locationtech.geomesa.arrow.TypeBindings
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding.Encoding
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding.Encoding.Encoding
import org.locationtech.geomesa.arrow.vector.impl.{AbstractLineStringVector, AbstractPointVector}
import org.locationtech.geomesa.features.serialization.ObjectType
import org.locationtech.geomesa.features.serialization.ObjectType.ObjectType
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.SimpleFeatureType

/**
  * Reads a simple feature attribute from an arrow vector
  */
trait ArrowAttributeReader {

  /**
    * Read an attribute from the ith feature in the simple feature vector
    *
    * @param i index of the feature to read
    * @return the attribute value
    */
  def apply(i: Int): AnyRef

  /**
    * Handle to the underlying field vector being read from
    *
    * @return
    */
  def vector: FieldVector

  /**
    *
    * @return
    */
  def getValueCount: Int = vector.getValueCount
}

trait ArrowDictionaryReader extends ArrowAttributeReader {

  /**
    * Gets the raw underlying value without dictionary decoding it
    *
    * @param i index of the feature to read
    * @return
    */
  def getEncoded(i: Int): Integer
}

object ArrowAttributeReader {

  /**
    * Reads an ID
    *
    * @param sft simple feature type
    * @param vector simple feature vector to read from
    * @param encoding encoding options
    * @return
    */
  def id(sft: SimpleFeatureType,
         vector: StructVector,
         encoding: SimpleFeatureEncoding = SimpleFeatureEncoding.Min): ArrowAttributeReader = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    def child = vector.getChild(SimpleFeatureVector.FeatureIdField)

    encoding.fids match {
      case None                             => ArrowAttributeReader.ArrowFeatureIdIncrementingReader
      case Some(Encoding.Min)               => new ArrowFeatureIdMinimalReader(child.asInstanceOf[IntVector])
      case Some(Encoding.Max) if sft.isUuid => new ArrowFeatureIdUuidReader(child.asInstanceOf[FixedSizeListVector])
      case Some(Encoding.Max)               => new ArrowStringReader(child.asInstanceOf[VarCharVector])
    }
  }

  /**
    * Creates a sequence of attribute readers based on the attributes of the simple feature type. There
    * will be one reader per attribute.
    *
    * @param sft simple feature type
    * @param vector simple feature vector to read from
    * @param dictionaries dictionaries, if any
    * @param encoding encoding options
    * @return sequence of readers
    */
  def apply(sft: SimpleFeatureType,
            vector: StructVector,
            dictionaries: Map[String, ArrowDictionary],
            encoding: SimpleFeatureEncoding = SimpleFeatureEncoding.Min): Seq[ArrowAttributeReader] = {
    import scala.collection.JavaConversions._
    sft.getAttributeDescriptors.map { descriptor =>
      val name = descriptor.getLocalName
      val dictionary = dictionaries.get(name).orElse(dictionaries.get(descriptor.getLocalName))
      apply(descriptor, vector.getChild(name), dictionary, encoding)
    }
  }

  def apply(descriptor: AttributeDescriptor,
            vector: FieldVector,
            dictionary: Option[ArrowDictionary],
            encoding: SimpleFeatureEncoding): ArrowAttributeReader = {
    apply(ObjectType.selectType(descriptor), vector, dictionary, encoding)
  }

  /**
    * Creates an attribute reader for a single attribute
    *
    * @param bindings object bindings, the attribute type plus any subtypes (e.g. for lists or maps)
    * @param vector the simple feature vector to read from
    * @param dictionary the dictionary for the attribute, if any
    * @param encoding encoding options
    * @return reader
    */
  def apply(bindings: Seq[ObjectType],
            vector: FieldVector,
            dictionary: Option[ArrowDictionary],
            encoding: SimpleFeatureEncoding): ArrowAttributeReader = {
    dictionary match {
      case None =>
        bindings.head match {
          case ObjectType.GEOMETRY => ArrowGeometryReader(vector, bindings(1), encoding.geometry)
          case ObjectType.DATE     => ArrowDateReader(vector, encoding.date)
          case ObjectType.STRING   => new ArrowStringReader(vector.asInstanceOf[VarCharVector])
          case ObjectType.INT      => new ArrowIntReader(vector.asInstanceOf[IntVector])
          case ObjectType.LONG     => new ArrowLongReader(vector.asInstanceOf[BigIntVector])
          case ObjectType.FLOAT    => new ArrowFloatReader(vector.asInstanceOf[Float4Vector])
          case ObjectType.DOUBLE   => new ArrowDoubleReader(vector.asInstanceOf[Float8Vector])
          case ObjectType.BOOLEAN  => new ArrowBooleanReader(vector.asInstanceOf[BitVector])
          case ObjectType.LIST     => new ArrowListReader(vector.asInstanceOf[ListVector], bindings(1), encoding)
          case ObjectType.MAP      => new ArrowMapReader(vector.asInstanceOf[StructVector], bindings(1), bindings(2), encoding)
          case ObjectType.BYTES    => new ArrowByteReader(vector.asInstanceOf[VarBinaryVector])
          case ObjectType.JSON     => new ArrowStringReader(vector.asInstanceOf[VarCharVector])
          case ObjectType.UUID     => new ArrowUuidReader(vector.asInstanceOf[FixedSizeListVector])
          case _ => throw new IllegalArgumentException(s"Unexpected object type ${bindings.head}")
        }

      case Some(dict) =>
        val dictionaryType = TypeBindings(bindings, encoding)
        vector match {
          case v: TinyIntVector  => new ArrowDictionaryByteReader(v, dict, dictionaryType)
          case v: SmallIntVector => new ArrowDictionaryShortReader(v, dict, dictionaryType)
          case v: IntVector      => new ArrowDictionaryIntReader(v, dict, dictionaryType)
          case _ => throw new IllegalArgumentException(s"Unexpected dictionary vector: $vector")
        }
    }
  }

  /**
    * Reads dictionary encoded bytes and converts them to the actual values
    */
  class ArrowDictionaryByteReader(override val vector: TinyIntVector,
                                  val dictionary: ArrowDictionary,
                                  val dictionaryType: TypeBindings) extends ArrowDictionaryReader {
    private val holder = new NullableTinyIntHolder

    override def apply(i: Int): AnyRef = {
      vector.get(i, holder)
      if (holder.isSet == 0) { null } else {
        dictionary.lookup(holder.value)
      }
    }

    override def getEncoded(i: Int): Integer = {
      vector.get(i, holder)
      if (holder.isSet == 0) { null } else { Int.box(holder.value) }
    }
  }

  /**
    * Reads dictionary encoded shorts and converts them to the actual values
    *
    */
  class ArrowDictionaryShortReader(override val vector: SmallIntVector,
                                   val dictionary: ArrowDictionary,
                                   val dictionaryType: TypeBindings) extends ArrowDictionaryReader {
    private val holder = new NullableSmallIntHolder

    override def apply(i: Int): AnyRef = {
      vector.get(i, holder)
      if (holder.isSet == 0) { null } else {
        dictionary.lookup(holder.value)
      }
    }

    override def getEncoded(i: Int): Integer = {
      vector.get(i, holder)
      if (holder.isSet == 0) { null } else { Int.box(holder.value) }
    }
  }

  /**
    * Reads dictionary encoded ints and converts them to the actual values
    *
    */
  class ArrowDictionaryIntReader(override val vector: IntVector,
                                 val dictionary: ArrowDictionary,
                                 val dictionaryType: TypeBindings) extends ArrowDictionaryReader {
    private val holder = new NullableIntHolder

    override def apply(i: Int): AnyRef = {
      vector.get(i, holder)
      if (holder.isSet == 0) { null } else {
        dictionary.lookup(holder.value)
      }
    }

    override def getEncoded(i: Int): Integer = {
      vector.get(i, holder)
      if (holder.isSet == 0) { null } else { Int.box(holder.value) }
    }
  }

  object ArrowGeometryReader {
    def apply(vector: FieldVector, binding: ObjectType, encoding: Encoding): ArrowAttributeReader = {
      if (binding == ObjectType.POINT) {
        val delegate: AbstractPointVector[_] = encoding match {
          case Encoding.Min => new PointFloatVector(vector.asInstanceOf[FixedSizeListVector])
          case Encoding.Max => new PointVector(vector.asInstanceOf[FixedSizeListVector])
        }
        new ArrowPointReader(vector, delegate)
      } else if (binding == ObjectType.LINESTRING) {
        val delegate: AbstractLineStringVector[_] = encoding match {
          case Encoding.Min => new LineStringFloatVector(vector.asInstanceOf[ListVector])
          case Encoding.Max => new LineStringVector(vector.asInstanceOf[ListVector])
        }
        new ArrowLineStringReader(vector, delegate)
      } else {
        val delegate: GeometryVector[_ <: Geometry, _] = if (binding == ObjectType.POLYGON) {
          encoding match {
            case Encoding.Min => new PolygonFloatVector(vector.asInstanceOf[ListVector])
            case Encoding.Max => new PolygonVector(vector.asInstanceOf[ListVector])
          }
        } else if (binding == ObjectType.MULTILINESTRING) {
          encoding match {
            case Encoding.Min => new MultiLineStringFloatVector(vector.asInstanceOf[ListVector])
            case Encoding.Max => new MultiLineStringVector(vector.asInstanceOf[ListVector])
          }
        } else if (binding == ObjectType.MULTIPOLYGON) {
          encoding match {
            case Encoding.Min => new MultiPolygonFloatVector(vector.asInstanceOf[ListVector])
            case Encoding.Max => new MultiPolygonVector(vector.asInstanceOf[ListVector])
          }
        } else if (binding == ObjectType.MULTIPOINT) {
          encoding match {
            case Encoding.Min => new MultiPointFloatVector(vector.asInstanceOf[ListVector])
            case Encoding.Max => new MultiPointVector(vector.asInstanceOf[ListVector])
          }
        } else if (binding == ObjectType.GEOMETRY_COLLECTION) {
          throw new NotImplementedError(s"Geometry type $binding is not supported")
        } else {
          throw new IllegalArgumentException(s"Expected geometry type, got $binding")
        }
        new ArrowGeometryReader(vector, delegate)
      }
    }
  }

  /**
    * Reads geometries - delegates to our JTS geometry vectors
    */
  class ArrowGeometryReader(override val vector: FieldVector, delegate: GeometryVector[_ <: Geometry, _])
      extends ArrowAttributeReader {
    override def apply(i: Int): AnyRef = delegate.get(i)
  }

  /**
    * Subclass with special methods for reading coordinate directly
    */
  class ArrowPointReader(override val vector: FieldVector, delegate: AbstractPointVector[_])
      extends ArrowAttributeReader {

    override def apply(i: Int): AnyRef = delegate.get(i)

    /**
      * Reads the first (y) ordinal for the given point
      *
      * @param i index of the point to read
      * @return y ordinal
      */
    def readPointY(i: Int): Double = delegate.getCoordinateY(i)

    /**
      * Reads the second (x) ordinal for the given point
      *
      * @param i index of the point to read
      * @return x ordinal
      */
    def readPointX(i: Int): Double = delegate.getCoordinateX(i)
  }

  /**
    * Subclass with special methods for reading coordinate directly
    */
  class ArrowLineStringReader(override val vector: FieldVector, delegate: AbstractLineStringVector[_])
      extends ArrowAttributeReader {

    override def apply(i: Int): AnyRef = delegate.get(i)

    /**
      * Gets the offsets for points in the ith line
      *
      * @param i index of the line to read offsets for
      * @return (offset start, offset end)
      */
    def readOffsets(i: Int): (Int, Int) = (delegate.getStartOffset(i), delegate.getEndOffset(i))

    /**
      * Reads the first (y) ordinal for the given point
      *
      * @param offset offset, from readOffsetStart/End, of the point to read
      * @return y ordinal
      */
    def readPointY(offset: Int): Double = delegate.getCoordinateY(offset)

    /**
      * Reads the second (x) ordinal for the given point
      *
      * @param offset offset, from readOffsetStart/End, of the point to read
      * @return x ordinal
      */
    def readPointX(offset: Int): Double = delegate.getCoordinateX(offset)
  }

  /**
    * Returns an incrementing Long to use as a feature id
    */
  object ArrowFeatureIdIncrementingReader extends ArrowAttributeReader {
    private val ids = new AtomicLong(0)
    override val vector: FieldVector = null
    override def apply(i: Int): AnyRef = ids.getAndIncrement.toString
    override def getValueCount: Int = 0
  }

  class ArrowFeatureIdUuidReader(vector: FixedSizeListVector) extends ArrowUuidReader(vector) {
    override def apply(i: Int): AnyRef = String.valueOf(super.apply(i))
  }

  class ArrowFeatureIdMinimalReader(vector: IntVector) extends ArrowIntReader(vector) {
    override def apply(i: Int): AnyRef = String.valueOf(super.apply(i))
  }

  class ArrowStringReader(override val vector: VarCharVector) extends ArrowAttributeReader {
    private val holder = new NullableVarCharHolder
    private var bytes = Array.empty[Byte]
    override def apply(i: Int): AnyRef = {
      vector.get(i, holder)
      if (holder.isSet == 0) { null } else {
        val length = holder.end - holder.start
        if (bytes.length < length) {
          bytes = Array.ofDim(math.ceil(length * 1.3).toInt)
        }
        holder.buffer.getBytes(holder.start, bytes, 0, length)
        new String(bytes, 0, length, StandardCharsets.UTF_8)
      }
    }
  }

  class ArrowIntReader(override val vector: IntVector) extends ArrowAttributeReader {
    override def apply(i: Int): AnyRef = vector.getObject(i)
  }

  class ArrowLongReader(override val vector: BigIntVector) extends ArrowAttributeReader {
    override def apply(i: Int): AnyRef = vector.getObject(i)
  }

  class ArrowFloatReader(override val vector: Float4Vector) extends ArrowAttributeReader {
    override def apply(i: Int): AnyRef = vector.getObject(i)
  }

  class ArrowDoubleReader(override val vector: Float8Vector) extends ArrowAttributeReader {
    override def apply(i: Int): AnyRef = vector.getObject(i)
  }

  class ArrowBooleanReader(override val vector: BitVector) extends ArrowAttributeReader {
    override def apply(i: Int): AnyRef = vector.getObject(i)
  }

  trait ArrowDateReader extends ArrowAttributeReader {
    def getTime(i: Int): Long
  }

  object ArrowDateReader {
    def apply(vector: FieldVector, encoding: Encoding): ArrowDateReader = {
      encoding match {
        case Encoding.Min => new ArrowDateSecondsReader(vector.asInstanceOf[IntVector])
        case Encoding.Max => new ArrowDateMillisReader(vector.asInstanceOf[BigIntVector])
      }
    }
  }

  class ArrowDateMillisReader(override val vector: BigIntVector) extends ArrowDateReader {
    private val holder = new NullableBigIntHolder
    override def apply(i: Int): AnyRef = {
      vector.get(i, holder)
      if (holder.isSet == 0) { null } else {
        new Date(holder.value)
      }
    }
    override def getTime(i: Int): Long = {
      vector.get(i, holder)
      if (holder.isSet == 0) { 0L } else {
        holder.value
      }
    }
  }

  class ArrowDateSecondsReader(override val vector: IntVector) extends ArrowDateReader {
    private val holder = new NullableIntHolder
    override def apply(i: Int): AnyRef = {
      vector.get(i, holder)
      if (holder.isSet == 0) { null } else {
        new Date(holder.value * 1000L)
      }
    }
    override def getTime(i: Int): Long = {
      vector.get(i, holder)
      if (holder.isSet == 0) { 0L } else {
        holder.value * 1000L
      }
    }
  }

  class ArrowByteReader(override val vector: VarBinaryVector) extends ArrowAttributeReader {
    override def apply(i: Int): AnyRef = vector.getObject(i)
  }

  class ArrowUuidReader(override val vector: FixedSizeListVector) extends ArrowAttributeReader {
    private val bits = vector.getChildrenFromFields.get(0).asInstanceOf[BigIntVector]
    override def apply(i: Int): AnyRef = {
      if (vector.isNull(i)) { null } else {
        val msb = bits.get(i * 2)
        val lsb = bits.get(i * 2 + 1)
        new UUID(msb, lsb)
      }
    }
  }

  class ArrowListReader(override val vector: ListVector, binding: ObjectType, encoding: SimpleFeatureEncoding)
      extends ArrowAttributeReader {
    private val reader = ArrowAttributeReader(Seq(binding), vector.getDataVector, None, encoding)
    override def apply(i: Int): AnyRef = {
      if (vector.isNull(i)) { null } else {
        // note: the offset buffer can be swapped out, so don't hold on to any references to it
        var offset = vector.getOffsetBuffer.getInt(i * BaseRepeatedValueVector.OFFSET_WIDTH)
        val end = vector.getOffsetBuffer.getInt((i + 1) * BaseRepeatedValueVector.OFFSET_WIDTH)
        val list = new java.util.ArrayList[AnyRef](end - offset)
        while (offset < end) {
          list.add(reader.apply(offset))
          offset += 1
        }
        list
      }
    }
  }

  class ArrowMapReader(override val vector: StructVector,
                       keyBinding: ObjectType,
                       valueBinding: ObjectType,
                       encoding: SimpleFeatureEncoding) extends ArrowAttributeReader {
    private val keyReader = ArrowAttributeReader(Seq(ObjectType.LIST, keyBinding), vector.getChild("k"), None, encoding)
    private val valueReader = ArrowAttributeReader(Seq(ObjectType.LIST, valueBinding), vector.getChild("v"), None, encoding)
    override def apply(i: Int): AnyRef = {
      if (vector.isNull(i)) { null } else {
        val keys = keyReader.apply(i).asInstanceOf[java.util.List[AnyRef]]
        val values = valueReader.apply(i).asInstanceOf[java.util.List[AnyRef]]
        val map = new java.util.HashMap[AnyRef, AnyRef](keys.size)
        var count = 0
        while (count < keys.size()) {
          map.put(keys.get(count), values.get(count))
          count += 1
        }
        map
      }
    }
  }
}
