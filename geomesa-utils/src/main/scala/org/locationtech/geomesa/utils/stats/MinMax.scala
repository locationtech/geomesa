/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.utils.stats

import java.util.Date

import com.vividsolutions.jts.geom.{Coordinate, Geometry}
import org.geotools.geometry.jts.JTSFactoryFinder
import org.opengis.feature.simple.SimpleFeature

import scala.reflect.ClassTag

/**
 * The MinMax stat merely returns the min/max of an attribute's values.
 * Works with dates, integers, longs, doubles, and floats.
 *
 * @param attribute attribute index for the attribute the histogram is being made for
 * @tparam T the type of the attribute the stat is targeting (needs to be comparable)
 */
class MinMax[T](val attribute: Int)(implicit val defaults: MinMax.MinMaxDefaults[T], ct: ClassTag[T]) extends Stat {

  override type S = MinMax[T]

  private [stats] var minValue: T = defaults.min
  private [stats] var maxValue: T = defaults.max

  lazy val stringify = Stat.stringifier(ct.runtimeClass)
  private lazy val jsonStringify = Stat.stringifier(ct.runtimeClass, json = true)

  override def observe(sf: SimpleFeature): Unit = {
    val value = sf.getAttribute(attribute).asInstanceOf[T]
    if (value != null) {
      minValue = defaults.min(value, minValue)
      maxValue = defaults.max(value, maxValue)
    }
  }

  // note: can't unobserve min/max without storing a lot more data
  override def unobserve(sf: SimpleFeature): Unit = {}

  override def +(other: MinMax[T]): MinMax[T] = {
    val plus = new MinMax(attribute)
    plus.minValue = this.minValue
    plus.maxValue = this.maxValue
    plus += other
    plus
  }

  override def +=(other: MinMax[T]): Unit = {
    if (other.isEmpty) {
      // no-op
    } else if (isEmpty) {
      minValue = other.minValue
      maxValue = other.maxValue
    } else {
      minValue = defaults.min(minValue, other.minValue)
      maxValue = defaults.max(maxValue, other.maxValue)
    }
  }

  def min: T = if (isEmpty) maxValue else minValue
  def max: T = if (isEmpty) minValue else maxValue
  def bounds: (T, T) = (min, max)

  override def toJson: String = {
    if (isEmpty) {
      """{ "min": null, "max": null }"""
    } else {
      s"""{ "min": ${jsonStringify(minValue)}, "max": ${jsonStringify(maxValue)} }"""
    }
  }

  override def isEmpty: Boolean = minValue == defaults.min

  override def clear(): Unit = {
    minValue = defaults.min
    maxValue = defaults.max
  }

  override def isEquivalent(other: Stat): Boolean = other match {
    case that: MinMax[T] => attribute == that.attribute && minValue == that.minValue && maxValue == that.maxValue
    case _ => false
  }
}

object MinMax {

  trait MinMaxDefaults[T] {
    def min: T
    def max: T
    def min(left: T, right: T): T
    def max(left: T, right: T): T
  }

  abstract class ComparableMinMax[T <: Comparable[T]] extends MinMaxDefaults[T] {
    override def min(left: T, right: T): T = if (left.compareTo(right) > 0) right else left
    override def max(left: T, right: T): T = if (left.compareTo(right) < 0) right else left
  }

  implicit object MinMaxString extends ComparableMinMax[String] {
    override val min: String = "~~~"
    override val max: String = ""
  }

  implicit object MinMaxInt extends ComparableMinMax[java.lang.Integer] {
    override val min: java.lang.Integer = java.lang.Integer.MAX_VALUE
    override val max: java.lang.Integer = java.lang.Integer.MIN_VALUE
  }

  implicit object MinMaxLong extends ComparableMinMax[java.lang.Long] {
    override val min: java.lang.Long = java.lang.Long.MAX_VALUE
    override val max: java.lang.Long = java.lang.Long.MIN_VALUE
  }

  implicit object MinMaxFloat extends ComparableMinMax[java.lang.Float] {
    override val min: java.lang.Float = java.lang.Float.MAX_VALUE
    override val max: java.lang.Float = java.lang.Float.MIN_VALUE
  }

  implicit object MinMaxDouble extends ComparableMinMax[java.lang.Double] {
    override val min: java.lang.Double = java.lang.Double.MAX_VALUE
    override val max: java.lang.Double = java.lang.Double.MIN_VALUE
  }

  implicit object MinMaxDate extends ComparableMinMax[Date] {
    override val min: Date = new Date(java.lang.Long.MAX_VALUE)
    override val max: Date = new Date(java.lang.Long.MIN_VALUE)
  }

  /**
    * Geometry min/max tracks the bounding box of each geometry, not the geometries themselves.
    */
  implicit object MinMaxGeometry extends MinMaxDefaults[Geometry] {

    private val gf = JTSFactoryFinder.getGeometryFactory

    override val min: Geometry = gf.createPoint(new Coordinate(180.0, 90.0))
    override val max: Geometry = gf.createPoint(new Coordinate(-180.0, -90.0))

    override def min(left: Geometry, right: Geometry): Geometry = {
      val (lx, ly) = { val e = left.getEnvelopeInternal; (e.getMinX, e.getMinY) }
      val (rx, ry) = { val e = right.getEnvelopeInternal; (e.getMinX, e.getMinY) }

      val x = math.min(lx, rx)
      val y = math.min(ly, ry)

      if (x == lx && y == ly) {
        left
      } else if (x == rx && y == ry) {
        right
      } else {
        gf.createPoint(new Coordinate(x, y))
      }
    }

    override def max(left: Geometry, right: Geometry): Geometry = {
      val (lx, ly) = { val e = left.getEnvelopeInternal; (e.getMaxX, e.getMaxY) }
      val (rx, ry) = { val e = right.getEnvelopeInternal; (e.getMaxX, e.getMaxY) }

      val x = math.max(lx, rx)
      val y = math.max(ly, ry)

      if (x == lx && y == ly) {
        left
      } else if (x == rx && y == ry) {
        right
      } else {
        gf.createPoint(new Coordinate(x, y))
      }
    }
  }
}