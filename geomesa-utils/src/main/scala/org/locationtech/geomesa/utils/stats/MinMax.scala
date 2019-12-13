/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.stats

import java.util.Date

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.jts.geom.{Coordinate, Geometry}
import org.geotools.geometry.jts.JTSFactoryFinder
import org.locationtech.geomesa.utils.clearspring.HyperLogLog
import org.locationtech.geomesa.utils.stats.MinMax.CardinalityBits
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.immutable.ListMap

/**
  * The MinMax stat merely returns the min/max of an attribute's values.
  * Works with dates, integers, longs, doubles, and floats.
  *
  * @param sft simple feature type
  * @param property property name for the attribute being min/maxed
  * @tparam T the type of the attribute the stat is targeting (needs to be comparable)
 */
class MinMax[T] private [stats] (val sft: SimpleFeatureType,
                                 val property: String,
                                 private [stats] var minValue: T,
                                 private [stats] var maxValue: T,
                                 private [stats] val hpp: HyperLogLog)
                                (implicit val defaults: MinMax.MinMaxDefaults[T])
    extends Stat with LazyLogging with Serializable {

  // use a secondary constructor instead of companion apply to allow mixin types (i.e. ImmutableStat)
  def this(sft: SimpleFeatureType, attribute: String)(implicit defaults: MinMax.MinMaxDefaults[T]) =
    this(sft, attribute, defaults.max, defaults.min, HyperLogLog(CardinalityBits))(defaults)

  override type S = MinMax[T]

  @deprecated("property")
  lazy val attribute: Int = i

  private val i = sft.indexOf(property)

  def min: T = if (isEmpty) { maxValue } else { minValue }
  def max: T = if (isEmpty) { minValue } else { maxValue }
  def bounds: (T, T) = (min, max)
  def cardinality: Long = hpp.cardinality()
  def tuple: (T, T, Long) = (min, max, cardinality)

  override def observe(sf: SimpleFeature): Unit = {
    val value = sf.getAttribute(i).asInstanceOf[T]
    if (value != null) {
      try {
        minValue = defaults.min(value, minValue)
        maxValue = defaults.max(value, maxValue)
        hpp.offer(value)
      } catch {
        case e: Exception => logger.warn(s"Error observing value '$value': ${e.toString}")
      }
    }
  }

  // note: can't unobserve min/max without storing a lot more data
  override def unobserve(sf: SimpleFeature): Unit = {}

  override def +(other: MinMax[T]): MinMax[T] = {
    if (other.isEmpty) {
      new MinMax[T](sft, property, minValue, maxValue, hpp.merge())
    } else if (this.isEmpty) {
      new MinMax[T](sft, property, other.minValue, other.maxValue, other.hpp.merge())
    } else {
      val plus = new MinMax[T](sft, property, minValue, maxValue, hpp.merge())
      plus += other
      plus
    }
  }

  override def +=(other: MinMax[T]): Unit = {
    if (other.isEmpty) {
      // no-op
    } else if (isEmpty) {
      minValue = other.minValue
      maxValue = other.maxValue
      hpp += other.hpp
    } else {
      minValue = defaults.min(minValue, other.minValue)
      maxValue = defaults.max(maxValue, other.maxValue)
      hpp += other.hpp
    }
  }

  override def toJsonObject: Any =
    if (isEmpty) {
      ListMap("min" -> null, "max" -> null, "cardinality" -> 0)
    } else {
      ListMap("min" -> minValue, "max" -> maxValue, "cardinality" -> cardinality)
    }

  override def isEmpty: Boolean = minValue == defaults.max

  override def clear(): Unit = {
    minValue = defaults.max
    maxValue = defaults.min
    java.util.Arrays.fill(hpp.registerSet.rawBits, 0)
  }

  override def isEquivalent(other: Stat): Boolean = other match {
    case that: MinMax[T] =>
      property == that.property && minValue == that.minValue &&
          maxValue == that.maxValue && cardinality == that.cardinality
    case _ => false
  }
}

object MinMax {

  val CardinalityBits: Int = 10

  trait MinMaxDefaults[T] {
    def min: T
    def max: T
    def min(left: T, right: T): T
    def max(left: T, right: T): T
  }

  object MinMaxDefaults {
    def apply[T](binding: Class[_]): MinMaxDefaults[T] = {
      if (binding == classOf[String]) {
        MinMaxString.asInstanceOf[MinMaxDefaults[T]]
      } else if (binding == classOf[Integer]) {
        MinMaxInt.asInstanceOf[MinMaxDefaults[T]]
      } else if (binding == classOf[java.lang.Long]) {
        MinMaxLong.asInstanceOf[MinMaxDefaults[T]]
      } else if (binding == classOf[java.lang.Float]) {
        MinMaxFloat.asInstanceOf[MinMaxDefaults[T]]
      } else if (binding == classOf[java.lang.Double]) {
        MinMaxDouble.asInstanceOf[MinMaxDefaults[T]]
      } else if (classOf[Date].isAssignableFrom(binding)) {
        MinMaxDate.asInstanceOf[MinMaxDefaults[T]]
      } else if (classOf[Geometry].isAssignableFrom(binding)) {
        MinMaxGeometry.asInstanceOf[MinMaxDefaults[T]]
      } else {
        throw new IllegalArgumentException(s"No implicit default available for type: $binding")
      }
    }
  }

  abstract class ComparableMinMax[T <: Comparable[T]] extends MinMaxDefaults[T] with Serializable {
    override def min(left: T, right: T): T = if (left.compareTo(right) > 0) right else left
    override def max(left: T, right: T): T = if (left.compareTo(right) < 0) right else left
  }

  implicit object MinMaxString extends ComparableMinMax[String] with Serializable {
    override val min: String = ""
    override val max: String = "\uFFFF\uFFFF\uFFFF"
  }

  implicit object MinMaxInt extends ComparableMinMax[Integer] with Serializable {
    override val min: Integer = Integer.MIN_VALUE
    override val max: Integer = Integer.MAX_VALUE
  }

  implicit object MinMaxLong extends ComparableMinMax[java.lang.Long] with Serializable {
    override val min: java.lang.Long = java.lang.Long.MIN_VALUE
    override val max: java.lang.Long = java.lang.Long.MAX_VALUE
  }

  implicit object MinMaxFloat extends ComparableMinMax[java.lang.Float] with Serializable {
    override val min: java.lang.Float = 0f - java.lang.Float.MAX_VALUE
    override val max: java.lang.Float = java.lang.Float.MAX_VALUE
  }

  implicit object MinMaxDouble extends ComparableMinMax[java.lang.Double] with Serializable  {
    override val min: java.lang.Double = 0d - java.lang.Double.MAX_VALUE
    override val max: java.lang.Double = java.lang.Double.MAX_VALUE
  }

  implicit object MinMaxDate extends ComparableMinMax[Date] with Serializable {
    override val min: Date = new Date(java.lang.Long.MIN_VALUE)
    override val max: Date = new Date(java.lang.Long.MAX_VALUE)
  }

  /**
    * Geometry min/max tracks the bounding box of each geometry, not the geometries themselves.
    */
  implicit object MinMaxGeometry extends MinMaxDefaults[Geometry] with Serializable {

    private val gf = JTSFactoryFinder.getGeometryFactory

    override val min: Geometry = gf.createPoint(new Coordinate(-180.0, -90.0))
    override val max: Geometry = gf.createPoint(new Coordinate(180.0, 90.0))

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
