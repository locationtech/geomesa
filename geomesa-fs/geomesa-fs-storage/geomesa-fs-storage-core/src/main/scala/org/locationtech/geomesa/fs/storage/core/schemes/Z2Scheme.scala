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
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.fs.storage.core.schemes.SpatialScheme.SpatialPartitionSchemeFactory
import org.locationtech.geomesa.utils.geotools.GeometryUtils
import org.locationtech.geomesa.zorder.sfcurve.IndexRange
import org.locationtech.jts.geom.Point

case class Z2Scheme(attribute: String, index: Int, bits: Int) extends SpatialScheme {

  import org.locationtech.geomesa.filter.{andFilters, ff}
  import org.locationtech.geomesa.utils.geotools.CRS_EPSG_4326

  require(bits % 4 == 0, s"Bit precision must be a multiple of 4, but received $bits")

  private val z2 = Z2SFC
  private val xRadius = (360d / math.pow(2, bits / 2)) / 2
  private val yRadius = (180d / math.pow(2, bits / 2)) / 2

  // number of hex digits used to represent our z value - bits = (xz2.g - partitionLevel) * 2, then divide by 4 to get hex
  private val digits = bits / 4

  override val name: String = s"${Z2Scheme.name}:attribute=$attribute:bits=$bits"

  override def getPartition(feature: SimpleFeature): PartitionKey = {
    val pt = feature.getAttribute(index).asInstanceOf[Point]
    val zValue = z2.index(pt.getX, pt.getY)
    PartitionKey(name, truncateToPartition(zValue))
  }

  override def getCoveringFilter(partition: PartitionKey): Filter = {
    val (x, y) = z2.invert(partition.value.toLong)
    val (xmin, xmax) = (x - xRadius, x + xRadius)
    val (ymin, ymax) = (y - yRadius, y + yRadius)
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
