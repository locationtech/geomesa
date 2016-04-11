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

  private [stats] var min: T = defaults.min
  private [stats] var max: T = defaults.max

  private lazy val stringify = Stat.stringifier(ct.runtimeClass, json = true)

  override def observe(sf: SimpleFeature): Unit = {
    val value = sf.getAttribute(attribute).asInstanceOf[T]
    if (value != null) {
      val (mn, mx) = defaults.minmax(value, min, max)
      min = mn
      max = mx
    }
  }

  override def +(other: MinMax[T]): MinMax[T] = {
    val plus = new MinMax(attribute)
    plus.min = this.min
    plus.max = this.max
    plus += other
    plus
  }

  override def +=(other: MinMax[T]): Unit = {
    if (!other.isEmpty) {
      if (isEmpty) {
        min = other.min
        max = other.max
      } else {
        Seq(other.min, other.max).foreach { value =>
          val (mn, mx) = defaults.minmax(value, min, max)
          min = mn
          max = mx
        }
      }
    }
  }

  def bounds: Option[(T, T)] = if (isEmpty) None else Some((min, max))

  override def toJson: String = {
    val (minValue, maxValue) = bounds.getOrElse((null, null))
    s"""{ "min": ${stringify(minValue)}, "max": ${stringify(maxValue)} }"""
  }

  override def isEmpty: Boolean = min == defaults.min

  override def clear(): Unit = {
    min = defaults.min
    max = defaults.max
  }

  override def equals(other: Any): Boolean = other match {
    case that: MinMax[T] => attribute == that.attribute && min == that.min && max == that.max
    case _ => false
  }

  override def hashCode(): Int =
    Seq(attribute, min, max).map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
}

object MinMax {

  trait MinMaxDefaults[T] {
    def min: T
    def max: T
    def minmax(value: T, min: T, max: T): (T, T)
  }

  abstract class ComparableMinMax[T <: Comparable[T]] extends MinMaxDefaults[T] {
    override def minmax(value: T, min: T, max: T): (T, T) = {
      val mn = if (value.compareTo(min) > 0) min else value
      val mx = if (value.compareTo(max) < 0) max else value
      (mn, mx)
    }
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

    override def minmax(value: Geometry, min: Geometry, max: Geometry): (Geometry, Geometry) = {

      val (xmin, ymin) = { val e = min.getEnvelopeInternal; (e.getMinX, e.getMinY) }
      val (xmax, ymax) = { val e = max.getEnvelopeInternal; (e.getMaxX, e.getMaxY) }

      val (vxmin, vymin, vxmax, vymax) = {
        val e = value.getEnvelopeInternal
        (e.getMinX, e.getMinY, e.getMaxX, e.getMaxY)
      }

      val mn = if (vxmin < xmin || vymin < ymin) {
        val x = if (vxmin < xmin) vxmin else xmin
        val y = if (vymin < ymin) vymin else ymin
        gf.createPoint(new Coordinate(x, y))
      } else {
        min
      }

      val mx = if (vxmax > xmax || vymax > ymax) {
        val x = if (vxmax > xmax) vxmax else xmax
        val y = if (vymax > ymax) vymax else ymax
        gf.createPoint(new Coordinate(x, y))
      } else {
        max
      }

      (mn, mx)
    }
  }
}