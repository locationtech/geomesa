/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
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
import org.joda.time.{DateTimeZone, LocalDateTime}
import org.locationtech.geomesa.arrow.TypeBindings
import org.locationtech.geomesa.arrow.vector.ArrowDictionary.HasArrowDictionary
import org.locationtech.geomesa.arrow.vector.GeometryVector.GeometryReader
import org.locationtech.geomesa.arrow.vector.LineStringVector.LineStringDoubleReader
import org.locationtech.geomesa.arrow.vector.MultiLineStringVector.MultiLineStringDoubleReader
import org.locationtech.geomesa.arrow.vector.MultiPointVector.MultiPointDoubleReader
import org.locationtech.geomesa.arrow.vector.MultiPolygonVector.MultiPolygonDoubleReader
import org.locationtech.geomesa.arrow.vector.PointVector.PointDoubleReader
import org.locationtech.geomesa.arrow.vector.PolygonVector.PolygonDoubleReader
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.EncodingPrecision.EncodingPrecision
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.{EncodingPrecision, SimpleFeatureEncoding}
import LineStringFloatVector.LineStringFloatReader
import MultiLineStringFloatVector.MultiLineStringFloatReader
import MultiPointFloatVector.MultiPointFloatReader
import MultiPolygonFloatVector.MultiPolygonFloatReader
import PointFloatVector.PointFloatReader
import PolygonFloatVector.PolygonFloatReader
import org.locationtech.geomesa.arrow.vector.impl.{AbstractLineStringVector, AbstractPointVector}
import org.locationtech.geomesa.features.serialization.ObjectType
import org.locationtech.geomesa.features.serialization.ObjectType.ObjectType
import org.locationtech.geomesa.utils.text.WKTUtils
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
}

trait ArrowDictionaryReader extends ArrowAttributeReader with HasArrowDictionary {

  /**
    * Gets the raw underlying value without dictionary decoding it
    *
    * @param i index of the feature to read
    * @return
    */
  def getEncoded(i: Int): Int
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
      ArrowAttributeReader(Seq(ObjectType.STRING), classOf[String], child, None, null)
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
      val classBinding = descriptor.getType.getBinding
      val (objectType, bindings) = ObjectType.selectType(classBinding, descriptor.getUserData)
      val dictionary = dictionaries.get(name).orElse(dictionaries.get(descriptor.getLocalName))
      apply(bindings.+:(objectType), classBinding, vector.getChild(name), dictionary, encoding)
    }
  }

  /**
    * Creates an attribute reader for a single attribute
    *
    * @param bindings object bindings, the attribute type plus any subtypes (e.g. for lists or maps)
    * @param classBinding the explicit class binding of the attribute
    * @param vector the simple feature vector to read from
    * @param dictionary the dictionary for the attribute, if any
    * @param encoding encoding options
    * @return reader
    */
  def apply(bindings: Seq[ObjectType],
            classBinding: Class[_],
            vector: FieldVector,
            dictionary: Option[ArrowDictionary],
            encoding: SimpleFeatureEncoding): ArrowAttributeReader = {
    val accessor = vector.getAccessor
    dictionary match {
      case None =>
        bindings.head match {
          case ObjectType.GEOMETRY => ArrowGeometryReader(vector, classBinding, encoding.geometry)
          case ObjectType.DATE     => ArrowDateReader(vector, encoding.date)
          case ObjectType.STRING   => new ArrowStringReader(accessor.asInstanceOf[NullableVarCharVector#Accessor])
          case ObjectType.INT      => new ArrowIntReader(accessor.asInstanceOf[NullableIntVector#Accessor])
          case ObjectType.LONG     => new ArrowLongReader(accessor.asInstanceOf[NullableBigIntVector#Accessor])
          case ObjectType.FLOAT    => new ArrowFloatReader(accessor.asInstanceOf[NullableFloat4Vector#Accessor])
          case ObjectType.DOUBLE   => new ArrowDoubleReader(accessor.asInstanceOf[NullableFloat8Vector#Accessor])
          case ObjectType.BOOLEAN  => new ArrowBooleanReader(accessor.asInstanceOf[NullableBitVector#Accessor])
          case ObjectType.LIST     => new ArrowListReader(accessor.asInstanceOf[ListVector#Accessor], bindings(1))
          case ObjectType.MAP      => new ArrowMapReader(accessor.asInstanceOf[NullableMapVector#Accessor], bindings(1), bindings(2))
          case ObjectType.BYTES    => new ArrowByteReader(accessor.asInstanceOf[NullableVarBinaryVector#Accessor])
          case ObjectType.JSON     => new ArrowStringReader(accessor.asInstanceOf[NullableVarCharVector#Accessor])
          case ObjectType.UUID     => new ArrowUuidReader(accessor.asInstanceOf[NullableVarCharVector#Accessor])
          case _ => throw new IllegalArgumentException(s"Unexpected object type ${bindings.head}")
        }

      case Some(dict) =>
        val dictionaryType = TypeBindings(bindings, classBinding, encoding)
        accessor match {
          case a: NullableTinyIntVector#Accessor  => new ArrowDictionaryByteReader(a, dict, dictionaryType)
          case a: NullableSmallIntVector#Accessor => new ArrowDictionaryShortReader(a, dict, dictionaryType)
          case a: NullableIntVector#Accessor      => new ArrowDictionaryIntReader(a, dict, dictionaryType)
          case _ => throw new IllegalArgumentException(s"Unexpected dictionary vector accessor: $accessor")
        }
    }
  }

  /**
    * Reads dictionary encoded bytes and converts them to the actual values
    */
  class ArrowDictionaryByteReader(accessor: NullableTinyIntVector#Accessor,
                                  val dictionary: ArrowDictionary,
                                  val dictionaryType: TypeBindings) extends ArrowDictionaryReader {
    private val holder = new NullableTinyIntHolder
    override def apply(i: Int): AnyRef = {
      accessor.get(i, holder)
      if (holder.isSet == 0) { null } else {
        dictionary.lookup(holder.value)
      }
    }

    override def getEncoded(i: Int): Int = accessor.get(i)
  }

  /**
    * Reads dictionary encoded shorts and converts them to the actual values
    *
    */
  class ArrowDictionaryShortReader(accessor: NullableSmallIntVector#Accessor,
                                   val dictionary: ArrowDictionary,
                                   val dictionaryType: TypeBindings) extends ArrowDictionaryReader {
    private val holder = new NullableSmallIntHolder
    override def apply(i: Int): AnyRef = {
      accessor.get(i, holder)
      if (holder.isSet == 0) { null } else {
        dictionary.lookup(holder.value)
      }
    }

    override def getEncoded(i: Int): Int = accessor.get(i)
  }

  /**
    * Reads dictionary encoded ints and converts them to the actual values
    *
    */
  class ArrowDictionaryIntReader(accessor: NullableIntVector#Accessor,
                                 val dictionary: ArrowDictionary,
                                 val dictionaryType: TypeBindings) extends ArrowDictionaryReader {
    private val holder = new NullableIntHolder
    override def apply(i: Int): AnyRef = {
      accessor.get(i, holder)
      if (holder.isSet == 0) { null } else {
        dictionary.lookup(holder.value)
      }
    }

    override def getEncoded(i: Int): Int = accessor.get(i)
  }

  object ArrowGeometryReader {
    def apply(vector: FieldVector, binding: Class[_], precision: EncodingPrecision): ArrowAttributeReader = {
      if (binding == classOf[Point]) {
        val delegate = precision match {
          case EncodingPrecision.Min => new PointFloatReader(vector.asInstanceOf[FixedSizeListVector])
          case EncodingPrecision.Max => new PointDoubleReader(vector.asInstanceOf[FixedSizeListVector])
        }
        new ArrowPointReader(delegate.asInstanceOf[AbstractPointVector.PointReader])
      } else if (binding == classOf[LineString]) {
        val delegate = precision match {
          case EncodingPrecision.Min => new LineStringFloatReader(vector.asInstanceOf[ListVector])
          case EncodingPrecision.Max => new LineStringDoubleReader(vector.asInstanceOf[ListVector])
        }
        new ArrowLineStringReader(delegate.asInstanceOf[AbstractLineStringVector.LineStringReader])
      } else {
        val delegate: GeometryReader[_ <: Geometry] = if (binding == classOf[Polygon]) {
          precision match {
            case EncodingPrecision.Min => new PolygonFloatReader(vector.asInstanceOf[ListVector])
            case EncodingPrecision.Max => new PolygonDoubleReader(vector.asInstanceOf[ListVector])
          }
        } else if (binding == classOf[MultiLineString]) {
          precision match {
            case EncodingPrecision.Min => new MultiLineStringFloatReader(vector.asInstanceOf[ListVector])
            case EncodingPrecision.Max => new MultiLineStringDoubleReader(vector.asInstanceOf[ListVector])
          }
        } else if (binding == classOf[MultiPolygon]) {
          precision match {
            case EncodingPrecision.Min => new MultiPolygonFloatReader(vector.asInstanceOf[ListVector])
            case EncodingPrecision.Max => new MultiPolygonDoubleReader(vector.asInstanceOf[ListVector])
          }
        } else if (binding == classOf[MultiPoint]) {
          precision match {
            case EncodingPrecision.Min => new MultiPointFloatReader(vector.asInstanceOf[ListVector])
            case EncodingPrecision.Max => new MultiPointDoubleReader(vector.asInstanceOf[ListVector])
          }
        } else if (classOf[Geometry].isAssignableFrom(binding)) {
          throw new NotImplementedError(s"Geometry type $binding is not supported")
        } else {
          throw new IllegalArgumentException(s"Expected geometry type, got $binding")
        }
        new ArrowGeometryReader(delegate)
      }
    }
  }

  /**
    * Reads geometries - delegates to our JTS geometry vectors
    */
  class ArrowGeometryReader(delegate: GeometryReader[_ <: Geometry]) extends ArrowAttributeReader {
    override def apply(i: Int): AnyRef = delegate.get(i)
  }

  /**
    * Subclass with special methods for reading coordinate directly
    */
  class ArrowPointReader(delegate: AbstractPointVector.PointReader) extends ArrowAttributeReader {

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
  class ArrowLineStringReader(delegate: AbstractLineStringVector.LineStringReader) extends ArrowAttributeReader {

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
    override def apply(i: Int): AnyRef = ids.getAndIncrement.toString
  }

  class ArrowStringReader(accessor: NullableVarCharVector#Accessor) extends ArrowAttributeReader {
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

  class ArrowIntReader(accessor: NullableIntVector#Accessor) extends ArrowAttributeReader {
    override def apply(i: Int): AnyRef = accessor.getObject(i)
  }

  class ArrowLongReader(accessor: NullableBigIntVector#Accessor) extends ArrowAttributeReader {
    override def apply(i: Int): AnyRef = accessor.getObject(i)
  }

  class ArrowFloatReader(accessor: NullableFloat4Vector#Accessor) extends ArrowAttributeReader {
    override def apply(i: Int): AnyRef = accessor.getObject(i)
  }

  class ArrowDoubleReader(accessor: NullableFloat8Vector#Accessor) extends ArrowAttributeReader {
    override def apply(i: Int): AnyRef = accessor.getObject(i)
  }

  class ArrowBooleanReader(accessor: NullableBitVector#Accessor) extends ArrowAttributeReader {
    override def apply(i: Int): AnyRef = accessor.getObject(i)
  }

  trait ArrowDateReader extends ArrowAttributeReader {
    def getTime(i: Int): Long
  }

  object ArrowDateReader {
    def apply(vector: FieldVector, precision: EncodingPrecision): ArrowDateReader = {
      precision match {
        case EncodingPrecision.Min => new ArrowDateSecondsReader(vector.asInstanceOf[NullableIntVector].getAccessor)
        case EncodingPrecision.Max => new ArrowDateMillisReader(vector.asInstanceOf[NullableBigIntVector].getAccessor)
      }
    }
  }

  class ArrowDateMillisReader(accessor: NullableBigIntVector#Accessor) extends ArrowDateReader {
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

  class ArrowDateSecondsReader(accessor: NullableIntVector#Accessor) extends ArrowDateReader {
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

  class ArrowListReader(accessor: ListVector#Accessor, binding: ObjectType) extends ArrowAttributeReader {
    import scala.collection.JavaConverters._
    private val convert: (AnyRef) => AnyRef = arrowConversion(binding)
    override def apply(i: Int): AnyRef =
      accessor.getObject(i).asInstanceOf[java.util.List[AnyRef]].asScala.map(convert).asJava
  }

  class ArrowMapReader(accessor: NullableMapVector#Accessor, keyBinding: ObjectType, valueBinding: ObjectType)
      extends ArrowAttributeReader {
    private val convertKey: (AnyRef) => AnyRef = arrowConversion(keyBinding)
    private val convertValue: (AnyRef) => AnyRef = arrowConversion(valueBinding)
    override def apply(i: Int): AnyRef = {
      val map    = accessor.getObject(i).asInstanceOf[java.util.Map[AnyRef, AnyRef]]
      val keys   = map.get("k").asInstanceOf[java.util.List[AnyRef]]
      val values = map.get("v").asInstanceOf[java.util.List[AnyRef]]
      val result = new java.util.HashMap[AnyRef, AnyRef]
      var j = 0
      while (j < keys.size) {
        result.put(convertKey(keys.get(j)), convertValue(values.get(j)))
        j += 1
      }
      result
    }
  }

  class ArrowByteReader(accessor: NullableVarBinaryVector#Accessor) extends ArrowAttributeReader {
    override def apply(i: Int): AnyRef = accessor.getObject(i)
  }

  class ArrowUuidReader(accessor: NullableVarCharVector#Accessor) extends ArrowStringReader(accessor) {
    override def apply(i: Int): AnyRef = {
      val string = super.apply(i).asInstanceOf[String]
      if (string == null) { null } else {
        UUID.fromString(string)
      }
    }
  }

  /**
    * Conversion for arrow accessor object types to simple feature type standard types.
    *
    * Note: geometry conversion is only correct for nested lists and maps
    *
    * @param binding object type being read
    * @return
    */
  private def arrowConversion(binding: ObjectType): (AnyRef)=> AnyRef = binding match {
    case ObjectType.STRING   => (v) => v.asInstanceOf[org.apache.arrow.vector.util.Text].toString
    case ObjectType.GEOMETRY => (v) => WKTUtils.read(v.asInstanceOf[org.apache.arrow.vector.util.Text].toString)
    case ObjectType.DATE     => (v) => v.asInstanceOf[LocalDateTime].toDate(DateTimeZone.UTC.toTimeZone)
    case ObjectType.JSON     => (v) => v.asInstanceOf[org.apache.arrow.vector.util.Text].toString
    case ObjectType.UUID     => (v) => UUID.fromString(v.asInstanceOf[org.apache.arrow.vector.util.Text].toString)
    case _                   => (v) => v
  }
}
