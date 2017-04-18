/***********************************************************************
* Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.arrow.vector

import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicLong
import java.util.{Date, UUID}

import com.vividsolutions.jts.geom._
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.{FixedSizeListVector, ListVector, NullableMapVector}
import org.joda.time.DateTime
import org.locationtech.geomesa.arrow.TypeBindings
import org.locationtech.geomesa.arrow.vector.ArrowDictionary.HasArrowDictionary
import org.locationtech.geomesa.arrow.vector.GeometryVector.GeometryReader
import org.locationtech.geomesa.arrow.vector.LineStringVector.LineStringDoubleReader
import org.locationtech.geomesa.arrow.vector.MultiLineStringVector.MultiLineStringDoubleReader
import org.locationtech.geomesa.arrow.vector.MultiPointVector.MultiPointDoubleReader
import org.locationtech.geomesa.arrow.vector.MultiPolygonVector.MultiPolygonDoubleReader
import org.locationtech.geomesa.arrow.vector.PointVector.PointDoubleReader
import org.locationtech.geomesa.arrow.vector.PolygonVector.PolygonDoubleReader
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.GeometryPrecision
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.GeometryPrecision.GeometryPrecision
import org.locationtech.geomesa.arrow.vector.floats.LineStringFloatVector.LineStringFloatReader
import org.locationtech.geomesa.arrow.vector.floats.MultiLineStringFloatVector.MultiLineStringFloatReader
import org.locationtech.geomesa.arrow.vector.floats.MultiPointFloatVector.MultiPointFloatReader
import org.locationtech.geomesa.arrow.vector.floats.MultiPolygonFloatVector.MultiPolygonFloatReader
import org.locationtech.geomesa.arrow.vector.floats.PointFloatVector.PointFloatReader
import org.locationtech.geomesa.arrow.vector.floats.PolygonFloatVector.PolygonFloatReader
import org.locationtech.geomesa.features.serialization.ObjectType
import org.locationtech.geomesa.features.serialization.ObjectType.ObjectType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeatureType

trait ArrowAttributeReader {
  def apply(i: Int): AnyRef
}

object ArrowAttributeReader {

  def id(vector: NullableMapVector, includeFids: Boolean): ArrowAttributeReader = {
    if (includeFids) {
      ArrowAttributeReader(Seq(ObjectType.STRING), classOf[String], vector.getChild("id"), None, null)
    } else {
      ArrowAttributeReader.ArrowIncrementingFeatureIdReader
    }
  }

  def apply(sft: SimpleFeatureType,
            vector: NullableMapVector,
            dictionaries: Map[String, ArrowDictionary],
            precision: GeometryPrecision = GeometryPrecision.Double): Seq[ArrowAttributeReader] = {
    import scala.collection.JavaConversions._
    sft.getAttributeDescriptors.map { descriptor =>
      val name = SimpleFeatureTypes.encodeDescriptor(sft, descriptor)
      val classBinding = descriptor.getType.getBinding
      val (objectType, bindings) = ObjectType.selectType(classBinding, descriptor.getUserData)
      val dictionary = dictionaries.get(name).orElse(dictionaries.get(descriptor.getLocalName))
      apply(bindings.+:(objectType), classBinding, vector.getChild(name), dictionary, precision)
    }
  }

  def apply(bindings: Seq[ObjectType],
            classBinding: Class[_],
            vector: FieldVector,
            dictionary: Option[ArrowDictionary],
            precision: GeometryPrecision): ArrowAttributeReader = {
    val accessor = vector.getAccessor
    dictionary match {
      case None =>
        bindings.head match {
          case ObjectType.STRING   => new ArrowStringReader(accessor.asInstanceOf[NullableVarCharVector#Accessor])
          case ObjectType.GEOMETRY => new ArrowGeometryReader(vector, classBinding, precision)
          case ObjectType.INT      => new ArrowIntReader(accessor.asInstanceOf[NullableIntVector#Accessor])
          case ObjectType.LONG     => new ArrowLongReader(accessor.asInstanceOf[NullableBigIntVector#Accessor])
          case ObjectType.FLOAT    => new ArrowFloatReader(accessor.asInstanceOf[NullableFloat4Vector#Accessor])
          case ObjectType.DOUBLE   => new ArrowDoubleReader(accessor.asInstanceOf[NullableFloat8Vector#Accessor])
          case ObjectType.BOOLEAN  => new ArrowBooleanReader(accessor.asInstanceOf[NullableBitVector#Accessor])
          case ObjectType.DATE     => new ArrowDateReader(accessor.asInstanceOf[NullableDateMilliVector#Accessor])
          case ObjectType.LIST     => new ArrowListReader(accessor.asInstanceOf[ListVector#Accessor], bindings(1))
          case ObjectType.MAP      => new ArrowMapReader(accessor.asInstanceOf[NullableMapVector#Accessor], bindings(1), bindings(2))
          case ObjectType.BYTES    => new ArrowByteReader(accessor.asInstanceOf[NullableVarBinaryVector#Accessor])
          case ObjectType.JSON     => new ArrowStringReader(accessor.asInstanceOf[NullableVarCharVector#Accessor])
          case ObjectType.UUID     => new ArrowUuidReader(accessor.asInstanceOf[NullableVarCharVector#Accessor])
          case _ => throw new IllegalArgumentException(s"Unexpected object type ${bindings.head}")
        }

      case Some(dict) =>
        val dictionaryType = TypeBindings(bindings, classBinding, precision)
        accessor match {
          case a: NullableTinyIntVector#Accessor  => new ArrowDictionaryByteReader(a, dict, dictionaryType)
          case a: NullableSmallIntVector#Accessor => new ArrowDictionaryShortReader(a, dict, dictionaryType)
          case a: NullableIntVector#Accessor      => new ArrowDictionaryIntReader(a, dict, dictionaryType)
          case _ => throw new IllegalArgumentException(s"Unexpected dictionary vector accessor: $accessor")
        }
    }
  }

  class ArrowDictionaryByteReader(accessor: NullableTinyIntVector#Accessor,
                                  val dictionary: ArrowDictionary,
                                  val dictionaryType: TypeBindings) extends ArrowAttributeReader with HasArrowDictionary {
    override def apply(i: Int): AnyRef = {
      if (accessor.isNull(i)) { null } else {
        dictionary.lookup(accessor.get(i))
      }
    }
  }

  class ArrowDictionaryShortReader(accessor: NullableSmallIntVector#Accessor,
                                   val dictionary: ArrowDictionary,
                                   val dictionaryType: TypeBindings) extends ArrowAttributeReader with HasArrowDictionary {
    override def apply(i: Int): AnyRef = {
      if (accessor.isNull(i)) { null } else {
        dictionary.lookup(accessor.get(i))
      }
    }
  }

  class ArrowDictionaryIntReader(accessor: NullableIntVector#Accessor,
                                 val dictionary: ArrowDictionary,
                                 val dictionaryType: TypeBindings) extends ArrowAttributeReader with HasArrowDictionary {
    override def apply(i: Int): AnyRef = {
      if (accessor.isNull(i)) { null } else {
        dictionary.lookup(accessor.get(i))
      }
    }
  }

  class ArrowGeometryReader(vector: FieldVector, binding: Class[_], precision: GeometryPrecision)
      extends ArrowAttributeReader {
    private val delegate: GeometryReader[_ <: Geometry] = if (binding == classOf[Point]) {
      precision match {
        case GeometryPrecision.Float  => new PointFloatReader(vector.asInstanceOf[FixedSizeListVector])
        case GeometryPrecision.Double => new PointDoubleReader(vector.asInstanceOf[FixedSizeListVector])
      }
    } else if (binding == classOf[LineString]) {
      precision match {
        case GeometryPrecision.Float  => new LineStringFloatReader(vector.asInstanceOf[ListVector])
        case GeometryPrecision.Double => new LineStringDoubleReader(vector.asInstanceOf[ListVector])
      }
    } else if (binding == classOf[Polygon]) {
      precision match {
        case GeometryPrecision.Float  => new PolygonFloatReader(vector.asInstanceOf[ListVector])
        case GeometryPrecision.Double => new PolygonDoubleReader(vector.asInstanceOf[ListVector])
      }
    } else if (binding == classOf[MultiLineString]) {
      precision match {
        case GeometryPrecision.Float  => new MultiLineStringFloatReader(vector.asInstanceOf[ListVector])
        case GeometryPrecision.Double => new MultiLineStringDoubleReader(vector.asInstanceOf[ListVector])
      }
    } else if (binding == classOf[MultiPolygon]) {
      precision match {
        case GeometryPrecision.Float  => new MultiPolygonFloatReader(vector.asInstanceOf[ListVector])
        case GeometryPrecision.Double => new MultiPolygonDoubleReader(vector.asInstanceOf[ListVector])
      }
    } else if (binding == classOf[MultiPoint]) {
      precision match {
        case GeometryPrecision.Float  => new MultiPointFloatReader(vector.asInstanceOf[ListVector])
        case GeometryPrecision.Double => new MultiPointDoubleReader(vector.asInstanceOf[ListVector])
      }
    } else if (classOf[Geometry].isAssignableFrom(binding)) {
      throw new NotImplementedError(s"Geometry type $binding is not supported")
    } else {
      throw new IllegalArgumentException(s"Expected geometry type, got $binding")
    }

    override def apply(i: Int): AnyRef = delegate.get(i)
  }

  object ArrowIncrementingFeatureIdReader extends ArrowAttributeReader {
    private val ids = new AtomicLong(0)
    override def apply(i: Int): AnyRef = ids.getAndIncrement.toString
  }

  class ArrowStringReader(accessor: NullableVarCharVector#Accessor) extends ArrowAttributeReader {
    override def apply(i: Int): AnyRef = {
      if (accessor.isNull(i)) { null } else {
        new String(accessor.get(i), StandardCharsets.UTF_8)
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

  class ArrowDateReader(accessor: NullableDateMilliVector#Accessor) extends ArrowAttributeReader {
    override def apply(i: Int): AnyRef = {
      if (accessor.isNull(i)) { null } else {
        new Date(accessor.get(i))
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
    case ObjectType.DATE     => (v) => v.asInstanceOf[DateTime].toDate // TODO
    case ObjectType.JSON     => (v) => v.asInstanceOf[org.apache.arrow.vector.util.Text].toString
    case ObjectType.UUID     => (v) => UUID.fromString(v.asInstanceOf[org.apache.arrow.vector.util.Text].toString)
    case _                   => (v) => v
  }
}
