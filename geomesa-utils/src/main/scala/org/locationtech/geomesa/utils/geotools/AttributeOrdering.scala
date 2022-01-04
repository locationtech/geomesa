/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools

import java.util.{Date, UUID}

import org.locationtech.geomesa.utils.geotools.ObjectType.ObjectType
import org.locationtech.jts.geom.Geometry
import org.opengis.feature.`type`.AttributeDescriptor

import scala.math.Ordering

/**
 * Ordering of simple feature attributes
 */
object AttributeOrdering {

  /**
   * An ordering for a particular attribute. Note that although the signature is AnyRef for ease of use
   * with the simple feature API, the actual values being sorted must correspond to the type of the attribute
   *
   * @param descriptor descriptor
   * @return
   */
  def apply(descriptor: AttributeDescriptor): Ordering[AnyRef] = apply(ObjectType.selectType(descriptor))

  /**
   * An ordering for a particular attribute. Note that although the signature is AnyRef for ease of use
   * with the simple feature API, the actual values being sorted must correspond to the type of the attribute
   *
   * @param bindings type bindings
   * @return
   */
  def apply(bindings: Seq[ObjectType]): Ordering[AnyRef] = {
    val ordering = bindings.head match {
      case ObjectType.STRING   => StringOrdering
      case ObjectType.INT      => IntOrdering
      case ObjectType.LONG     => LongOrdering
      case ObjectType.FLOAT    => FloatOrdering
      case ObjectType.DOUBLE   => DoubleOrdering
      case ObjectType.BOOLEAN  => BooleanOrdering
      case ObjectType.DATE     => DateOrdering
      case ObjectType.UUID     => UuidOrdering
      case ObjectType.GEOMETRY => GeometryOrdering
      case ObjectType.BYTES    => BytesOrdering
      case ObjectType.LIST     => list(bindings.last)
      case ObjectType.MAP      => throw new NotImplementedError("Ordering for Map-type attributes is not supported")
      case b                   => throw new NotImplementedError(s"Unexpected attribute type: $b")
    }
    ordering.asInstanceOf[Ordering[AnyRef]]
  }

  private def list(binding: ObjectType): Ordering[AnyRef] = {
    val ordering = binding match {
      case ObjectType.STRING   => StringListOrdering
      case ObjectType.INT      => IntListOrdering
      case ObjectType.LONG     => LongListOrdering
      case ObjectType.FLOAT    => FloatListOrdering
      case ObjectType.DOUBLE   => DoubleListOrdering
      case ObjectType.BOOLEAN  => BooleanListOrdering
      case ObjectType.DATE     => DateListOrdering
      case ObjectType.UUID     => UuidListOrdering
      case ObjectType.GEOMETRY => GeometryListOrdering
      case ObjectType.BYTES    => BytesListOrdering
      case b                   => throw new NotImplementedError(s"Unexpected attribute type: List[$b]")
    }
    ordering.asInstanceOf[Ordering[AnyRef]]
  }

  val StringOrdering   : Ordering[String]            = new NullOrdering(Ordering.String)
  val IntOrdering      : Ordering[Integer]           = new NullOrdering(Ordering.ordered[Integer])
  val LongOrdering     : Ordering[java.lang.Long]    = new NullOrdering(Ordering.ordered[java.lang.Long])
  val FloatOrdering    : Ordering[java.lang.Float]   = new NullOrdering(Ordering.ordered[java.lang.Float])
  val DoubleOrdering   : Ordering[java.lang.Double]  = new NullOrdering(Ordering.ordered[java.lang.Double])
  val BooleanOrdering  : Ordering[java.lang.Boolean] = new NullOrdering(Ordering.ordered[java.lang.Boolean])
  val DateOrdering     : Ordering[Date]              = new NullOrdering(Ordering.ordered[Date])
  val UuidOrdering     : Ordering[UUID]              = new NullOrdering(Ordering.ordered[UUID])
  val GeometryOrdering : Ordering[Geometry]          = new NullOrdering(new GeometryOrdering())
  val BytesOrdering    : Ordering[Array[Byte]]       = new NullOrdering(new BytesOrdering())

  val StringListOrdering   : Ordering[java.util.List[String]]            = ListOrdering(StringOrdering)
  val IntListOrdering      : Ordering[java.util.List[Integer]]           = ListOrdering(IntOrdering)
  val LongListOrdering     : Ordering[java.util.List[java.lang.Long]]    = ListOrdering(LongOrdering)
  val FloatListOrdering    : Ordering[java.util.List[java.lang.Float]]   = ListOrdering(FloatOrdering)
  val DoubleListOrdering   : Ordering[java.util.List[java.lang.Double]]  = ListOrdering(DoubleOrdering)
  val BooleanListOrdering  : Ordering[java.util.List[java.lang.Boolean]] = ListOrdering(BooleanOrdering)
  val DateListOrdering     : Ordering[java.util.List[Date]]              = ListOrdering(DateOrdering)
  val UuidListOrdering     : Ordering[java.util.List[UUID]]              = ListOrdering(UuidOrdering)
  val GeometryListOrdering : Ordering[java.util.List[Geometry]]          = ListOrdering(GeometryOrdering)
  val BytesListOrdering    : Ordering[java.util.List[Array[Byte]]]       = ListOrdering(BytesOrdering)

  private class GeometryOrdering extends Ordering[Geometry] {
    override def compare(x: Geometry, y: Geometry): Int = x.compareTo(y)
  }

  private class BytesOrdering extends Ordering[Array[Byte]] {
    override def compare(x: Array[Byte], y: Array[Byte]): Int = {
      val len = math.min(x.length, y.length)
      var i = 0
      while (i < len) {
        val res = java.lang.Byte.compare(x(i), y(i))
        if (res != 0) {
          return res
        }
        i += 1
      }
      Integer.compare(x.length, y.length)
    }
  }

  private object ListOrdering {
    def apply[T](delegate: Ordering[T]): Ordering[java.util.List[T]] = new NullOrdering(new ListOrdering(delegate))
  }

  private class ListOrdering[T](delegate: Ordering[T]) extends Ordering[java.util.List[T]] {
    override def compare(x: java.util.List[T], y: java.util.List[T]): Int = {
      val len = math.min(x.size, y.size)
      var i = 0
      while (i < len) {
        val res = delegate.compare(x.get(i), y.get(i))
        if (res != 0) {
          return res
        }
        i += 1
      }
      Integer.compare(x.size, y.size)
    }
  }

  private class NullOrdering[T](delegate: Ordering[T]) extends Ordering[T] {
    override def compare(x: T, y: T): Int = {
      if (x == null) {
        if (y == null) { 0 } else { -1 }
      } else if (y == null) {
        1
      } else {
        delegate.compare(x, y)
      }
    }
  }
}
