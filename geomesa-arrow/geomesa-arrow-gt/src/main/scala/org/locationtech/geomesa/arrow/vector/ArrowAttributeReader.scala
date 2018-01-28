/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.vector

import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicLong
import java.util.{Date, UUID}

import com.vividsolutions.jts.geom._
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.{FixedSizeListVector, ListVector, NullableMapVector}
import org.apache.arrow.vector.holders._
import org.locationtech.geomesa.arrow.TypeBindings
import org.locationtech.geomesa.arrow.vector.GeometryVector.GeometryReader
import org.locationtech.geomesa.arrow.vector.LineStringFloatVector.LineStringFloatReader
import org.locationtech.geomesa.arrow.vector.LineStringVector.LineStringDoubleReader
import org.locationtech.geomesa.arrow.vector.MultiLineStringFloatVector.MultiLineStringFloatReader
import org.locationtech.geomesa.arrow.vector.MultiLineStringVector.MultiLineStringDoubleReader
import org.locationtech.geomesa.arrow.vector.MultiPointFloatVector.MultiPointFloatReader
import org.locationtech.geomesa.arrow.vector.MultiPointVector.MultiPointDoubleReader
import org.locationtech.geomesa.arrow.vector.MultiPolygonFloatVector.MultiPolygonFloatReader
import org.locationtech.geomesa.arrow.vector.MultiPolygonVector.MultiPolygonDoubleReader
import org.locationtech.geomesa.arrow.vector.PointFloatVector.PointFloatReader
import org.locationtech.geomesa.arrow.vector.PointVector.PointDoubleReader
import org.locationtech.geomesa.arrow.vector.PolygonFloatVector.PolygonFloatReader
import org.locationtech.geomesa.arrow.vector.PolygonVector.PolygonDoubleReader
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.EncodingPrecision.EncodingPrecision
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.{EncodingPrecision, SimpleFeatureEncoding}
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
  def getValueCount: Int = vector.getAccessor.getValueCount
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
    * @param vector simple feature vector
    * @param includeFids whether ids are included in the vector or not. If not, will return an incrementing ID value.
    * @return
    */
  def id(vector: NullableMapVector, includeFids: Boolean): ArrowAttributeReader = {
    if (includeFids) {
      val child = vector.getChild(SimpleFeatureVector.FeatureIdField)
      ArrowAttributeReader(Seq(ObjectType.STRING), child, None, null)
    } else {
      ArrowAttributeReader.ArrowIncrementingFeatureIdReader
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
            vector: NullableMapVector,
            dictionaries: Map[String, ArrowDictionary],
            encoding: SimpleFeatureEncoding = SimpleFeatureEncoding.min(false)): Seq[ArrowAttributeReader] = {
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
          case ObjectType.STRING   => new ArrowStringReader(vector.asInstanceOf[NullableVarCharVector])
          case ObjectType.INT      => new ArrowIntReader(vector.asInstanceOf[NullableIntVector])
          case ObjectType.LONG     => new ArrowLongReader(vector.asInstanceOf[NullableBigIntVector])
          case ObjectType.FLOAT    => new ArrowFloatReader(vector.asInstanceOf[NullableFloat4Vector])
          case ObjectType.DOUBLE   => new ArrowDoubleReader(vector.asInstanceOf[NullableFloat8Vector])
          case ObjectType.BOOLEAN  => new ArrowBooleanReader(vector.asInstanceOf[NullableBitVector])
          case ObjectType.LIST     => new ArrowListReader(vector.asInstanceOf[ListVector], bindings(1), encoding)
          case ObjectType.MAP      => new ArrowMapReader(vector.asInstanceOf[NullableMapVector], bindings(1), bindings(2), encoding)
          case ObjectType.BYTES    => new ArrowByteReader(vector.asInstanceOf[NullableVarBinaryVector])
          case ObjectType.JSON     => new ArrowStringReader(vector.asInstanceOf[NullableVarCharVector])
          case ObjectType.UUID     => new ArrowUuidReader(vector.asInstanceOf[NullableVarCharVector])
          case _ => throw new IllegalArgumentException(s"Unexpected object type ${bindings.head}")
        }

      case Some(dict) =>
        val dictionaryType = TypeBindings(bindings, encoding)
        vector match {
          case v: NullableTinyIntVector  => new ArrowDictionaryByteReader(v, dict, dictionaryType)
          case v: NullableSmallIntVector => new ArrowDictionaryShortReader(v, dict, dictionaryType)
          case v: NullableIntVector      => new ArrowDictionaryIntReader(v, dict, dictionaryType)
          case _ => throw new IllegalArgumentException(s"Unexpected dictionary vector: $vector")
        }
    }
  }

  /**
    * Reads dictionary encoded bytes and converts them to the actual values
    */
  class ArrowDictionaryByteReader(override val vector: NullableTinyIntVector,
                                  val dictionary: ArrowDictionary,
                                  val dictionaryType: TypeBindings) extends ArrowDictionaryReader {
    private val accessor = vector.getAccessor
    private val holder = new NullableTinyIntHolder
    override def apply(i: Int): AnyRef = {
      accessor.get(i, holder)
      if (holder.isSet == 0) { null } else {
        dictionary.lookup(holder.value)
      }
    }

    override def getEncoded(i: Int): Integer = {
      accessor.get(i, holder)
      if (holder.isSet == 0) { null } else { Int.box(holder.value) }
    }
  }

  /**
    * Reads dictionary encoded shorts and converts them to the actual values
    *
    */
  class ArrowDictionaryShortReader(override val vector: NullableSmallIntVector,
                                   val dictionary: ArrowDictionary,
                                   val dictionaryType: TypeBindings) extends ArrowDictionaryReader {
    private val accessor = vector.getAccessor
    private val holder = new NullableSmallIntHolder
    override def apply(i: Int): AnyRef = {
      accessor.get(i, holder)
      if (holder.isSet == 0) { null } else {
        dictionary.lookup(holder.value)
      }
    }

    override def getEncoded(i: Int): Integer = {
      accessor.get(i, holder)
      if (holder.isSet == 0) { null } else { Int.box(holder.value) }
    }
  }

  /**
    * Reads dictionary encoded ints and converts them to the actual values
    *
    */
  class ArrowDictionaryIntReader(override val vector: NullableIntVector,
                                 val dictionary: ArrowDictionary,
                                 val dictionaryType: TypeBindings) extends ArrowDictionaryReader {
    private val accessor = vector.getAccessor
    private val holder = new NullableIntHolder
    override def apply(i: Int): AnyRef = {
      accessor.get(i, holder)
      if (holder.isSet == 0) { null } else {
        dictionary.lookup(holder.value)
      }
    }

    override def getEncoded(i: Int): Integer = {
      accessor.get(i, holder)
      if (holder.isSet == 0) { null } else { Int.box(holder.value) }
    }
  }

  object ArrowGeometryReader {
    def apply(vector: FieldVector, binding: ObjectType, precision: EncodingPrecision): ArrowAttributeReader = {
      if (binding == ObjectType.POINT) {
        val delegate = precision match {
          case EncodingPrecision.Min => new PointFloatReader(vector.asInstanceOf[FixedSizeListVector])
          case EncodingPrecision.Max => new PointDoubleReader(vector.asInstanceOf[FixedSizeListVector])
        }
        new ArrowPointReader(vector, delegate.asInstanceOf[AbstractPointVector.PointReader])
      } else if (binding == ObjectType.LINESTRING) {
        val delegate = precision match {
          case EncodingPrecision.Min => new LineStringFloatReader(vector.asInstanceOf[ListVector])
          case EncodingPrecision.Max => new LineStringDoubleReader(vector.asInstanceOf[ListVector])
        }
        new ArrowLineStringReader(vector, delegate.asInstanceOf[AbstractLineStringVector.LineStringReader])
      } else {
        val delegate: GeometryReader[_ <: Geometry] = if (binding == ObjectType.POLYGON) {
          precision match {
            case EncodingPrecision.Min => new PolygonFloatReader(vector.asInstanceOf[ListVector])
            case EncodingPrecision.Max => new PolygonDoubleReader(vector.asInstanceOf[ListVector])
          }
        } else if (binding == ObjectType.MULTILINESTRING) {
          precision match {
            case EncodingPrecision.Min => new MultiLineStringFloatReader(vector.asInstanceOf[ListVector])
            case EncodingPrecision.Max => new MultiLineStringDoubleReader(vector.asInstanceOf[ListVector])
          }
        } else if (binding == ObjectType.MULTIPOLYGON) {
          precision match {
            case EncodingPrecision.Min => new MultiPolygonFloatReader(vector.asInstanceOf[ListVector])
            case EncodingPrecision.Max => new MultiPolygonDoubleReader(vector.asInstanceOf[ListVector])
          }
        } else if (binding == ObjectType.MULTIPOINT) {
          precision match {
            case EncodingPrecision.Min => new MultiPointFloatReader(vector.asInstanceOf[ListVector])
            case EncodingPrecision.Max => new MultiPointDoubleReader(vector.asInstanceOf[ListVector])
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
  class ArrowGeometryReader(override val vector: FieldVector, delegate: GeometryReader[_ <: Geometry])
      extends ArrowAttributeReader {
    override def apply(i: Int): AnyRef = delegate.get(i)
  }

  /**
    * Subclass with special methods for reading coordinate directly
    */
  class ArrowPointReader(override val vector: FieldVector, delegate: AbstractPointVector.PointReader)
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
  class ArrowLineStringReader(override val vector: FieldVector, delegate: AbstractLineStringVector.LineStringReader)
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
  object ArrowIncrementingFeatureIdReader extends ArrowAttributeReader {
    private val ids = new AtomicLong(0)
    override val vector: FieldVector = null
    override def apply(i: Int): AnyRef = ids.getAndIncrement.toString
    override def getValueCount: Int = 0
  }

  class ArrowStringReader(override val vector: NullableVarCharVector) extends ArrowAttributeReader {
    private val accessor = vector.getAccessor
    private val holder = new NullableVarCharHolder
    private var bytes = Array.empty[Byte]
    override def apply(i: Int): AnyRef = {
      accessor.get(i, holder)
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

  class ArrowIntReader(override val vector: NullableIntVector) extends ArrowAttributeReader {
    private val accessor = vector.getAccessor
    override def apply(i: Int): AnyRef = accessor.getObject(i)
  }

  class ArrowLongReader(override val vector: NullableBigIntVector) extends ArrowAttributeReader {
    private val accessor = vector.getAccessor
    override def apply(i: Int): AnyRef = accessor.getObject(i)
  }

  class ArrowFloatReader(override val vector: NullableFloat4Vector) extends ArrowAttributeReader {
    private val accessor = vector.getAccessor
    override def apply(i: Int): AnyRef = accessor.getObject(i)
  }

  class ArrowDoubleReader(override val vector: NullableFloat8Vector) extends ArrowAttributeReader {
    private val accessor = vector.getAccessor
    override def apply(i: Int): AnyRef = accessor.getObject(i)
  }

  class ArrowBooleanReader(override val vector: NullableBitVector) extends ArrowAttributeReader {
    private val accessor = vector.getAccessor
    override def apply(i: Int): AnyRef = accessor.getObject(i)
  }

  trait ArrowDateReader extends ArrowAttributeReader {
    def getTime(i: Int): Long
  }

  object ArrowDateReader {
    def apply(vector: FieldVector, precision: EncodingPrecision): ArrowDateReader = {
      precision match {
        case EncodingPrecision.Min => new ArrowDateSecondsReader(vector.asInstanceOf[NullableIntVector])
        case EncodingPrecision.Max => new ArrowDateMillisReader(vector.asInstanceOf[NullableBigIntVector])
      }
    }
  }

  class ArrowDateMillisReader(override val vector: NullableBigIntVector) extends ArrowDateReader {
    private val accessor = vector.getAccessor
    private val holder = new NullableBigIntHolder
    override def apply(i: Int): AnyRef = {
      accessor.get(i, holder)
      if (holder.isSet == 0) { null } else {
        new Date(holder.value)
      }
    }
    override def getTime(i: Int): Long = {
      accessor.get(i, holder)
      if (holder.isSet == 0) { 0L } else {
        holder.value
      }
    }
  }

  class ArrowDateSecondsReader(override val vector: NullableIntVector) extends ArrowDateReader {
    private val accessor = vector.getAccessor
    private val holder = new NullableIntHolder
    override def apply(i: Int): AnyRef = {
      accessor.get(i, holder)
      if (holder.isSet == 0) { null } else {
        new Date(holder.value * 1000L)
      }
    }
    override def getTime(i: Int): Long = {
      accessor.get(i, holder)
      if (holder.isSet == 0) { 0L } else {
        holder.value * 1000L
      }
    }
  }

  class ArrowByteReader(override val vector: NullableVarBinaryVector) extends ArrowAttributeReader {
    private val accessor = vector.getAccessor
    override def apply(i: Int): AnyRef = accessor.getObject(i)
  }

  class ArrowUuidReader(override val vector: NullableVarCharVector) extends ArrowStringReader(vector) {
    private val accessor = vector.getAccessor
    override def apply(i: Int): AnyRef = {
      val string = super.apply(i).asInstanceOf[String]
      if (string == null) { null } else {
        UUID.fromString(string)
      }
    }
  }

  class ArrowListReader(override val vector: ListVector, binding: ObjectType, encoding: SimpleFeatureEncoding)
      extends ArrowAttributeReader {
    private val offsets = vector.getFieldInnerVectors.get(1).asInstanceOf[UInt4Vector].getAccessor
    private val reader = ArrowAttributeReader(Seq(binding), vector.getDataVector, None, encoding)
    override def apply(i: Int): AnyRef = {
      if (vector.getAccessor.isNull(i)) { null } else {
        var offset = offsets.get(i)
        val end = offsets.get(i + 1)
        val list = new java.util.ArrayList[AnyRef](end - offset)
        while (offset < end) {
          list.add(reader.apply(offset))
          offset += 1
        }
        list
      }
    }
  }

  class ArrowMapReader(override val vector: NullableMapVector,
                       keyBinding: ObjectType,
                       valueBinding: ObjectType,
                       encoding: SimpleFeatureEncoding) extends ArrowAttributeReader {
    private val keyReader = ArrowAttributeReader(Seq(ObjectType.LIST, keyBinding), vector.getChild("k"), None, encoding)
    private val valueReader = ArrowAttributeReader(Seq(ObjectType.LIST, valueBinding), vector.getChild("v"), None, encoding)
    override def apply(i: Int): AnyRef = {
      if (vector.getAccessor.isNull(i)) { null } else {
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
