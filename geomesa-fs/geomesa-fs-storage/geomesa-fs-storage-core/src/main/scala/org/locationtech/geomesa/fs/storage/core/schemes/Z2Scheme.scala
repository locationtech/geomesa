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
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.curve.Z2SFC
import org.locationtech.geomesa.fs.storage.core.schemes.SpatialScheme.SpatialPartitionSchemeFactory
import org.locationtech.geomesa.zorder.sfcurve.IndexRange
import org.locationtech.jts.geom.Point

case class Z2Scheme(attribute: String, index: Int, bits: Int) extends SpatialScheme {

  import org.locationtech.geomesa.filter.{andFilters, ff}
  import org.locationtech.geomesa.utils.geotools.CRS_EPSG_4326

  require(bits % 4 == 0, s"Bit precision must be a multiple of 4, but received $bits")

  private val z2 = Z2SFC

  // number of hex digits used to represent our z value
  private val digits = bits / 4

  // in getCoveringFilter, expand the bbox by half a cell to account for round-trip errors
  // invert returns the center of the cell, so we need to expand to the edges
  // cell size at 31-bit precision: lon = 360.0 / 2^31, lat = 180.0 / 2^31
  private val xDelta = ((z2.lon.max - z2.lon.min) / (1L << z2.precision)) / 2.0
  private val yDelta = ((z2.lat.max - z2.lat.min) / (1L << z2.precision)) / 2.0

  override val name: String = s"${Z2Scheme.name}:attribute=$attribute:bits=$bits"

  override def getPartition(feature: SimpleFeature): PartitionKey = {
    val pt = feature.getAttribute(index).asInstanceOf[Point]
    val zValue = z2.index(pt.getX, pt.getY)
    PartitionKey(name, truncateToPartition(zValue))
  }

  override def getCoveringFilter(partition: PartitionKey): Filter = {
    val lower = z2.hexDecode(partition.value.padTo(16, '0'))
    val upper = z2.hexDecode(partition.value.padTo(16, 'f'))
    val (xmin, ymin) = z2.invert(lower) match { case (xmin, ymin) => (xmin - xDelta, ymin - yDelta) }
    val (xmax, ymax) = z2.invert(upper) match { case (xmax, ymax) => (xmax + xDelta, ymax + yDelta) }

    val bbox = ff.bbox(ff.property(attribute), new ReferencedEnvelope(xmin, xmax, ymin, ymax, CRS_EPSG_4326))
    // account for borders between z-cells (make upper bounds exclusive except on the upper-right edge)
    val xExclusive = if (xmax == z2.lon.max) { None } else {
      Some(ff.less(ff.function("getX", ff.property(attribute)), ff.literal(xmax)))
    }
    val yExclusive = if (ymax == z2.lat.max) { None } else {
      Some(ff.less(ff.function("getY", ff.property(attribute)), ff.literal(ymax)))
    }
    andFilters(Seq(bbox) ++ xExclusive ++ yExclusive)
  }

  override protected def zRanges(xy: Seq[(Double, Double, Double, Double)]): Seq[IndexRange] = z2.ranges(xy)
  override protected def truncateToPartition(z: Long): String = z2.hexEncode(z).take(digits)
}

object Z2Scheme extends SpatialPartitionSchemeFactory[Point]("z2") {
  override def buildPartitionScheme(bits: Int, geom: String, geomIndex: Int): PartitionScheme =
    Z2Scheme(geom, geomIndex, bits)
}
