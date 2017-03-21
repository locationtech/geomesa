/*******************************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ******************************************************************************/

package org.locationtech.geomesa.arrow.vector

import java.nio.charset.StandardCharsets
import java.util.{Date, UUID}

import com.vividsolutions.jts.geom.{Geometry, Point}
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.{ListVector, NullableMapVector}
import org.locationtech.geomesa.arrow.vector.reader.{GeometryReader, PointReader}
import org.locationtech.geomesa.features.serialization.ObjectType
import org.locationtech.geomesa.features.serialization.ObjectType.ObjectType
import org.opengis.feature.`type`.AttributeDescriptor

trait ArrowAttributeReader {
  def apply(i: Int): AnyRef
}

object ArrowAttributeReader {

  import scala.collection.JavaConversions._

  def apply(descriptor: AttributeDescriptor,
            vector: NullableMapVector,
            dictionary: Option[ArrowDictionary]): ArrowAttributeReader = {
    val classBinding = descriptor.getType.getBinding
    val (objectType, bindings) = ObjectType.selectType(classBinding, descriptor.getUserData)
    apply(descriptor.getLocalName, bindings.+:(objectType), classBinding, vector, dictionary)
  }

  def apply(name: String,
            bindings: Seq[ObjectType],
            classBinding: Class[_],
            vector: NullableMapVector,
            dictionary: Option[ArrowDictionary]): ArrowAttributeReader = {
    val child = vector.getChild(name)
    val accessor = child.getAccessor
    dictionary match {
      case None =>
        bindings.head match {
          case ObjectType.STRING   => new ArrowStringReader(accessor.asInstanceOf[NullableVarCharVector#Accessor])
          case ObjectType.GEOMETRY => new ArrowGeometryReader(child.asInstanceOf[NullableMapVector], classBinding)
          case ObjectType.INT      => new ArrowIntReader(accessor.asInstanceOf[NullableIntVector#Accessor])
          case ObjectType.LONG     => new ArrowLongReader(accessor.asInstanceOf[NullableBigIntVector#Accessor])
          case ObjectType.FLOAT    => new ArrowFloatReader(accessor.asInstanceOf[NullableFloat4Vector#Accessor])
          case ObjectType.DOUBLE   => new ArrowDoubleReader(accessor.asInstanceOf[NullableFloat8Vector#Accessor])
          case ObjectType.BOOLEAN  => new ArrowBooleanReader(accessor.asInstanceOf[NullableBitVector#Accessor])
          case ObjectType.DATE     => new ArrowDateReader(accessor.asInstanceOf[NullableDateVector#Accessor])
          case ObjectType.LIST     => new ArrowListReader(accessor.asInstanceOf[ListVector#Accessor], bindings(1))
          case ObjectType.MAP      => new ArrowMapReader(accessor.asInstanceOf[NullableMapVector#Accessor], bindings(1), bindings(2))
          case ObjectType.BYTES    => new ArrowByteReader(accessor.asInstanceOf[NullableVarBinaryVector#Accessor])
          case ObjectType.JSON     => new ArrowStringReader(accessor.asInstanceOf[NullableVarCharVector#Accessor])
          case ObjectType.UUID     => new ArrowUuidReader(accessor.asInstanceOf[NullableVarCharVector#Accessor])
          case _ => throw new IllegalArgumentException(s"Unexpected object type ${bindings.head}")
        }

      case Some(dict) =>
        bindings.head match {
          case ObjectType.STRING =>
            accessor match {
              case a: NullableTinyIntVector#Accessor  => new ArrowDictionaryByteReader(a, dict)
              case a: NullableSmallIntVector#Accessor => new ArrowDictionaryShortReader(a, dict)
              case a: NullableIntVector#Accessor      => new ArrowDictionaryIntReader(a, dict)
              case _ => throw new IllegalArgumentException(s"Unexpected dictionary vector accessor: $accessor")
            }

          case _ => throw new IllegalArgumentException(s"Dictionary only supported for string type: ${bindings.head}")
        }
    }
  }

  class ArrowDictionaryByteReader(accessor: NullableTinyIntVector#Accessor, dictionary: ArrowDictionary)
      extends ArrowAttributeReader {
    override def apply(i: Int): AnyRef = {
      if (accessor.isNull(i)) { null } else {
        dictionary.lookup(accessor.get(i))
      }
    }
  }

  class ArrowDictionaryShortReader(accessor: NullableSmallIntVector#Accessor, dictionary: ArrowDictionary)
      extends ArrowAttributeReader {
    override def apply(i: Int): AnyRef = {
      if (accessor.isNull(i)) { null } else {
        dictionary.lookup(accessor.get(i))
      }
    }
  }

  class ArrowDictionaryIntReader(accessor: NullableIntVector#Accessor, dictionary: ArrowDictionary)
      extends ArrowAttributeReader {
    override def apply(i: Int): AnyRef = {
      if (accessor.isNull(i)) { null } else {
        dictionary.lookup(accessor.get(i))
      }
    }
  }

  class ArrowGeometryReader(vector: NullableMapVector, binding: Class[_]) extends ArrowAttributeReader {
    private val delegate: GeometryReader[Geometry] = if (binding == classOf[Point]) {
      new PointReader(vector).asInstanceOf[GeometryReader[Geometry]]
    } else if (classOf[Geometry].isAssignableFrom(binding)) {
      throw new NotImplementedError("Currently only supports points")
    } else {
      throw new IllegalArgumentException(s"Expected geometry type, got $binding")
    }

    override def apply(i: Int): AnyRef = delegate.get(i)
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

  class ArrowDateReader(accessor: NullableDateVector#Accessor) extends ArrowAttributeReader {
    override def apply(i: Int): AnyRef = {
      if (accessor.isNull(i)) { null } else {
        new Date(accessor.get(i))
      }
    }
  }

  class ArrowListReader(accessor: ListVector#Accessor, binding: ObjectType) extends ArrowAttributeReader {
    // TODO need to translate list objects into appropriate types for dates and strings
    override def apply(i: Int): AnyRef = accessor.getObject(i)
  }

  class ArrowMapReader(accessor: NullableMapVector#Accessor, keyBinding: ObjectType, valueBinding: ObjectType)
      extends ArrowAttributeReader {
    // TODO pretty sure this is going to be wrong
    override def apply(i: Int): AnyRef = accessor.getObject(i)
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

}
