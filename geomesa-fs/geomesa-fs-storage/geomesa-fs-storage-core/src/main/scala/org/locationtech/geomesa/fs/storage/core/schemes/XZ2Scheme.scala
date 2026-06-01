/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core
package schemes

import org.geotools.api.feature.simple.SimpleFeature
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.curve.XZ2SFC
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.filter.function.XZ2Function
import org.locationtech.geomesa.fs.storage.core.schemes.SpatialScheme.SpatialPartitionSchemeFactory
import org.locationtech.geomesa.fs.storage.core.schemes.XZ2Scheme.incrementHex
import org.locationtech.geomesa.utils.geotools.GeometryUtils
import org.locationtech.jts.geom.Geometry

import scala.annotation.tailrec

/**
 * XZ2 spatial scheme.
 *
 * This scheme uses a fixed high-resolution curve for indexing, then truncates the value to create partition groups,
 * in order to align with the iceberg 'truncate' transforms.
 *
 * Index values are (max) 7-digit hex-encoded longs, as we're only using 25 bits for the high-resolution curve,
 * we can fit it in 7 digits.
 *
 * @param attribute name of the attribute being partitioned
 * @param index index in the sft of the attribute being partitioned
 * @param bits number of bits of resolution used for partitioning
 */
case class XZ2Scheme(attribute: String, index: Int, bits: Int) extends PartitionScheme {

  import FilterHelper.ff

  require(bits % 4 == 0, s"Bit precision must be a multiple of 4, but received $bits")

  private val xz2 = XZ2SFC

  // partition level derived from bits parameter
  // each level adds 2 bits (4 quadrants)
  private val partitionLevel = (bits / 2).toShort
  // number of hex digits used to represent our z value - bits = (xz2.g - partitionLevel) * 2, then divide by 4 to get hex
  private val digits = xz2.hexDigits - ((xz2.g - partitionLevel) / 2)

  lazy private val wholeWorldRanges = Some(generateRanges(Seq((-180, -90, 180, 90))))

  override val name: String = s"${XZ2Scheme.name}:attribute=$attribute:bits=$bits"

  override def getPartition(feature: SimpleFeature): PartitionKey = {
    val envelope = feature.getAttribute(index).asInstanceOf[Geometry].getEnvelopeInternal
    val zValue = xz2.index(envelope.getMinX, envelope.getMinY, envelope.getMaxX, envelope.getMaxY)
    PartitionKey(name, truncateToPartition(zValue))
  }

  override def getCoveringFilter(partition: PartitionKey): Filter = {
    // TODO maybe we can improve this with *some* kind of bbox?
    val zPrefix = partition.value
    val lower = zPrefix.padTo(xz2.hexDigits, '0')
    val upper = zPrefix.padTo(xz2.hexDigits, 'f')
    ff.between(ff.function(XZ2Function.FunctionName.getName), ff.literal(lower), ff.literal(upper))
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

  override def getPartitionsForFilter(filter: Filter): Option[Seq[PartitionKey]] = {
    getRangesForFilter(filter).orElse(wholeWorldRanges).map { ranges =>
      ranges.flatMap { range =>
        Iterator.iterate(range.lower)(incrementHex).takeWhile(_ < range.upper).map(PartitionKey(name, _))
      }
    }
  }

  private def generateRanges(xy: Seq[(Double, Double, Double, Double)]): Seq[PartitionRange] = {
    val builder = new RangeBuilder()
    xz2.ranges(xy).foreach { range =>
      val lower = truncateToPartition(range.lower)
      // index ranges are inclusive, but partition ranges are exclusive
      val upper = incrementHex(truncateToPartition(range.upper))
      builder += PartitionRange(name, lower, upper)
    }
    builder.result()
  }

  // truncates a full-resolution index to a partition group ID
  private def truncateToPartition(fullIndex: Long): String = xz2.hexEncode(fullIndex).take(digits)
}

object XZ2Scheme extends SpatialPartitionSchemeFactory[Geometry]("xz2") {

  override def buildPartitionScheme(bits: Int, geom: String, geomIndex: Int): PartitionScheme =
    XZ2Scheme(geom, geomIndex, bits)

  private def incrementHex(hex: String): String = incrementHex(hex, hex.length - 1)

  @tailrec
  private def incrementHex(hex: String, pos: Int): String = {
    val c = hex.charAt(pos)
    if (c != 'f') {
      hex.substring(0, pos) + (c + 1).toChar + hex.substring(pos + 1)
    } else if (pos == 0) {
      hex + '0' // note: this isn't actually incrementing the value but should sort after all the valid hex values
    } else {
      incrementHex(hex.substring(0, pos) + '0' + hex.substring(pos + 1), pos - 1)
    }
  }
}
