/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core
package schemes

import org.apache.iceberg.expressions.{Expression, Expressions}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.fs.storage.core.schemes.PartitionScheme.EnumeratedScheme
import org.locationtech.jts.geom.Geometry

import java.util.regex.Pattern
import scala.reflect.ClassTag

trait SpatialScheme extends PartitionScheme with EnumeratedScheme {

  /**
   * Number of digits in the hex-encoded space-filling curve values
   *
   * @return
   */
  protected def digits: Int

  /**
   * Gets the full (not truncated) hex-encoded ranges for a query
   *
   * @param bounds query bounds, in the form (xmin, ymin, xmax, ymax)
   * @return
   */
  protected def hexRanges(bounds: Seq[(Double, Double, Double, Double)]): Seq[(String, String)]

  /**
   * Gets an expression that will match the specified bounds
   *
   * @param bounds spatial bounds, in the form (xmin, ymin, xmax, ymax). Multiple bounds are logically OR'd together
   * @return
   */
  def getCoveringExpression(bounds: Seq[(Double, Double, Double, Double)]): Expression = {
    // truncate and merge any overlapping ranges
    val allRanges = hexRanges(bounds).map(PartitionRange.apply).distinct.sortBy(_.lower)

    val inValues = new java.util.ArrayList[String]()
    var expression: Expression = Expressions.alwaysFalse()

    def addPartitionRange(r: PartitionRange): Unit = {
      if (r.lower == r.upper) {
        inValues.add(r.lower)
      } else {
        val gte = Expressions.greaterThanOrEqual(Expressions.truncate[String](column, digits), r.lower)
        val lte = Expressions.lessThanOrEqual(Expressions.truncate[String](column, digits), r.upper)
        expression = Expressions.or(expression, Expressions.and(gte, lte))
      }
    }

    val last = allRanges.reduce { (left, right) =>
      left.merge(right) match {
        case None => addPartitionRange(left); right
        case Some(merged) => merged
      }
    }
    addPartitionRange(last)

    if (!inValues.isEmpty) {
      expression = Expressions.or(expression, Expressions.in(Expressions.truncate[String](column, digits), inValues))
    }

    expression
  }

  override def getCoveringExpression(partition: PartitionKey): Expression =
    Expressions.equal[String](Expressions.truncate[String](column, digits), partition.value)

  override def partitions: Seq[PartitionKey] = {
    val keys = (1 to digits).foldLeft(Seq("")) { (accumulated, _) =>
      for {
        str <- accumulated
        char <- SpatialScheme.HexChars
      } yield {
        str + char
      }
    }
    keys.map(PartitionKey(name, _))
  }

  /**
   * Ranged bounds, used for filtering on partitions
   *
   * @param lower lower bound, inclusive
   * @param upper upper bound, exclusive
   */
  private case class PartitionRange(lower: String, upper: String) {

    /**
     * Attempt to merge two bounds. Only overlapping bounds will result in a successful merge. Trying to merge
     * bounds from a different partition scheme is a logical error.
     *
     * @param other bounds to merge
     * @return
     */
    def merge(other: PartitionRange): Option[PartitionRange] = {
      if (lower <= other.lower) {
        if (upper >= other.upper) {
          Some(this)
        } else if (upper >= other.lower) {
          Some(PartitionRange(lower, other.upper))
        } else {
          None
        }
      } else if (lower > other.upper) {
        None
      } else if (upper >= other.upper) {
        Some(PartitionRange(other.lower, upper))
      } else {
        Some(other)
      }
    }
  }

  private object PartitionRange {
    def apply(bounds: (String, String)): PartitionRange = PartitionRange(bounds._1.take(digits), bounds._2.take(digits))
  }
}

object SpatialScheme {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  val HexChars: Seq[Char] = Range(0, 10).map(_.toString.charAt(0)) ++ Seq.tabulate(6)(i => ('a' + i).toChar)

  abstract class SpatialPartitionSchemeFactory[T <: Geometry : ClassTag](val name: String) extends PartitionSchemeFactory {

    private val namePattern: Pattern = Pattern.compile(s"$name-([0-9]+)bits?:?")

    override def load(sft: SimpleFeatureType, scheme: String): Option[PartitionScheme] = {
      val opts = SchemeOpts(scheme)
      lazy val matcher = namePattern.matcher(scheme)

      def build(resolution: Short): Option[PartitionScheme] = {
        val geom = opts.getSingle("attribute").orElse(Option(sft.getGeomField)).orNull
        require(geom != null, s"Spatial schemes requires an attribute to be specified with 'attribute=<attribute>'")
        val index = attributeIndex(sft, geom, Some(implicitly[ClassTag[T]].runtimeClass))
        Some(buildPartitionScheme(resolution, geom, index))
      }

      if (opts.name == this.name) {
        val bits = opts.getSingle("bits").map(_.toShort).getOrElse {
          throw new IllegalArgumentException(s"Spatial schemes requires a resolution to be specified with 'bits=<resolution>'")
        }
        build(bits)
      } else if (matcher.matches()) {
        build(matcher.group(1).toShort)
      } else {
        None
      }
    }

    def buildPartitionScheme(bits: Int, geom: String, geomIndex: Int): PartitionScheme
  }
}
