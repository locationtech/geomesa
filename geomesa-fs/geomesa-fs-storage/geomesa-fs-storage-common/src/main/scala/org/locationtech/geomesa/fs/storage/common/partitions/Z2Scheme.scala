/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.partitions

import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.curve.Z2SFC
import org.locationtech.geomesa.fs.storage.common.partitions.SpatialScheme.SpatialPartitionSchemeFactory
import org.locationtech.jts.geom.Point
import org.locationtech.sfcurve.IndexRange
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter

case class Z2Scheme(bits: Int, geom: String, geomIndex: Int) extends SpatialScheme(bits, geom) {

  import org.locationtech.geomesa.filter.{andFilters, ff}
  import org.locationtech.geomesa.utils.geotools.CRS_EPSG_4326

  private val xyBits = bits / 2
  private val z2 = new Z2SFC(xyBits)
  private val xRadius = (360d / math.pow(2, xyBits)) / 2
  private val yRadius = (180d / math.pow(2, xyBits)) / 2

  override def pattern: String = s"$bits-bit-z2"

  override def getPartitionName(feature: SimpleFeature): String = {
    val pt = feature.getAttribute(geomIndex).asInstanceOf[Point]
    z2.index(pt.getX, pt.getY).formatted(format)
  }

  override def getCoveringFilter(partition: String): Filter = {
    val (x, y) = z2.invert(partition.toLong)
    val (xmin, xmax) = (x - xRadius, x + xRadius)
    val (ymin, ymax) = (y - yRadius, y + yRadius)
    val bbox = ff.bbox(ff.property(geom), new ReferencedEnvelope(xmin, xmax, ymin, ymax, CRS_EPSG_4326))
    // account for borders between z-cells (make upper bounds exclusive except on the upper-right edge)
    val xExclusive = if (xmax == z2.lon.max) { None } else {
      Some(ff.less(ff.function("getX", ff.property(geom)), ff.literal(xmax)))
    }
    val yExclusive = if (ymax == z2.lat.max) { None } else {
      Some(ff.less(ff.function("getY", ff.property(geom)), ff.literal(ymax)))
    }
    andFilters(Seq(bbox) ++ xExclusive ++ yExclusive)
  }

  override protected def digits(bits: Int): Int = math.ceil(bits * math.log10(2)).toInt

  override protected def generateRanges(xy: Seq[(Double, Double, Double, Double)]): Seq[IndexRange] = z2.ranges(xy)
}

object Z2Scheme {

  val Name = "z2"

  class Z2PartitionSchemeFactory extends SpatialPartitionSchemeFactory(Name) {
    override def buildPartitionScheme(bits: Int, geom: String, geomIndex: Int): SpatialScheme =
      Z2Scheme(bits, geom, geomIndex)
  }
}
