/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core
package schemes

import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.fs.storage.core.{PartitionScheme, PartitionSchemeFactory}
import org.locationtech.geomesa.utils.geotools.GeometryUtils
import org.locationtech.geomesa.utils.text.StringSerialization
import org.locationtech.geomesa.zorder.sfcurve.IndexRange
import org.locationtech.jts.geom.Geometry

import java.util.regex.Pattern
import scala.annotation.tailrec
import scala.reflect.ClassTag

trait SpatialScheme extends PartitionScheme {

  lazy private val wholeWorldRanges = Some(generateRanges(Seq((-180, -90, 180, 90))))

  override def getPartitionsForFilter(filter: Filter): Option[Seq[PartitionKey]] = {
    getRangesForFilter(filter).orElse(wholeWorldRanges).map { ranges =>
      ranges.flatMap { range =>
        Iterator.iterate(range.lower)(incrementHex).takeWhile(_ < range.upper).map(PartitionKey(name, _))
      }
    }
  }

  override def getRangesForFilter(filter: Filter): Option[Seq[PartitionRange]] = {
    val geometries = FilterHelper.extractGeometries(filter, attribute, intersect = true)
    if (geometries.isEmpty) {
      None
    } else if (geometries.disjoint) {
      Some(Seq.empty)
    } else {
      Some(generateRanges(geometries.values.map(GeometryUtils.bounds)))
    }
  }

  /**
   * Get z ranges from the underlying curve
   *
   * @param xy bbox queries
   * @return
   */
  protected def zRanges(xy: Seq[(Double, Double, Double, Double)]): Seq[IndexRange]

  /**
   * Truncates a full-resolution index to a partition group ID
   *
   * @param z z value
   * @return
   */
  protected def truncateToPartition(z: Long): String

  /**
   * Generate ranges for a query
   *
   * @param xy query bboxes
   * @return
   */
  private def generateRanges(xy: Seq[(Double, Double, Double, Double)]): Seq[PartitionRange] = {
    val builder = new RangeBuilder()
    zRanges(xy).foreach { range =>
      val lower = truncateToPartition(range.lower)
      // index ranges are inclusive, but partition ranges are exclusive
      val upper = incrementHex(truncateToPartition(range.upper))
      builder += PartitionRange(name, lower, upper)
    }
    builder.result()
  }

  /**
   * Increment a hex value, used for upper-level exclusive ranges. Note that in terminal cases (e.g. `ffff`),
   * an extra 'z' will be added, which works for our use case because it sorts after the original value
   *
   * @param hex hex value to increment
   * @return
   */
  private def incrementHex(hex: String): String = incrementHex(hex, hex.length - 1)

  @tailrec
  private def incrementHex(hex: String, pos: Int): String = {
    val c = hex.charAt(pos)
    if (c != 'f') {
      val bump = if (c == '9') { 'a' } else { (c + 1).toChar }
      hex.substring(0, pos) + bump + hex.substring(pos + 1)
    } else if (pos == 0) {
      hex + 'z' // note: this isn't actually incrementing the value but should sort after all the valid hex values
    } else {
      incrementHex(hex.substring(0, pos) + '0' + hex.substring(pos + 1), pos - 1)
    }
  }
}

object SpatialScheme {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

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


  /**
   * Holder for a z-value field, along with a reference back to the original geometry field
   *
   * @param geometry name of the original geometry field being covered
   * @param zValue name of the z-value field
   */
  case class ZValueField(geometry: String, zValue: String)

  object ZValueField {

    val ZValueFieldPrefix = "__"
    val Z2ValueFieldSuffix = "_z2__"
    val XZ2ValueFieldSuffix = "_xz2__"

    def z2(geometry: String, encoded: Boolean = false): ZValueField = {
      val geom = if (encoded) { geometry } else { StringSerialization.alphaNumericSafeString(geometry) }
      val zValue = s"$ZValueFieldPrefix$geom$Z2ValueFieldSuffix"
      ZValueField(geom, zValue)
    }

    def xz2(geometry: String, encoded: Boolean = false): ZValueField = {
      val geom = if (encoded) { geometry } else { StringSerialization.alphaNumericSafeString(geometry) }
      val zValue = s"$ZValueFieldPrefix$geom$XZ2ValueFieldSuffix"
      ZValueField(geom, zValue)
    }

    /**
     * Creates a field name based on a z-value field
     *
     * @param field name of a potential z-value field
     * @return
     */
    def fromFieldName(field: String): Option[ZValueField] = {
      if (field.startsWith(ZValueFieldPrefix)) {
        Seq(Z2ValueFieldSuffix, XZ2ValueFieldSuffix).collectFirst {
          case suffix if field.endsWith(suffix) =>
            ZValueField(field.substring(ZValueFieldPrefix.length, field.length - suffix.length), field)
        }
      } else {
        None
      }
    }
  }
}
