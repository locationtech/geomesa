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

case class Z2Scheme(attribute: String, index: Int, bits: Int) extends PartitionScheme {

  import org.locationtech.geomesa.filter.{andFilters, ff}
  import org.locationtech.geomesa.utils.geotools.CRS_EPSG_4326

  require(bits % 2 == 0, s"Bit precision must be an even number, but received $bits")

  private val xyBits = bits / 2
  private val z2 = new Z2SFC(xyBits)
  private val xRadius = (360d / math.pow(2, xyBits)) / 2
  private val yRadius = (180d / math.pow(2, xyBits)) / 2
  private val format = s"%0${digits(bits)}d"

  lazy private val wholeWorldRanges = Some(z2.ranges(Seq((-180, -90, 180, 90))))

  override val name: String = s"${Z2Scheme.name}:attribute=$attribute:bits=$bits"

  override def getPartition(feature: SimpleFeature): PartitionKey = {
    val pt = feature.getAttribute(index).asInstanceOf[Point]
    PartitionKey(name, format.format(z2.index(pt.getX, pt.getY)))
  }

  override def getRangesForFilter(filter: Filter): Option[Seq[PartitionRange]] = {
    getIndexRanges(filter).map { ranges =>
      val builder = new RangeBuilder()
      ranges.foreach { range =>
        val lower = format.format(range.lower)
        val upper = format.format(range.upper + 1)
        builder += PartitionRange(name, lower, upper)
      }
      builder.result()
    }
  }

  override def getPartitionsForFilter(filter: Filter): Option[Seq[PartitionKey]] = {
    getIndexRanges(filter).orElse(wholeWorldRanges).map { ranges =>
      ranges.flatMap { range =>
        val lower = range.lower
        val steps = 1 + (range.upper - lower).toInt
        Seq.tabulate(steps)(i => PartitionKey(name, format.format(lower + i)))
      }
    }
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

  private def getIndexRanges(filter: Filter): Option[Seq[IndexRange]] = {
    val geometries = FilterHelper.extractGeometries(filter, attribute, intersect = true)
    if (geometries.isEmpty) {
      None
    } else if (geometries.disjoint) {
      Some(Seq.empty)
    } else {
      Some(z2.ranges(geometries.values.map(GeometryUtils.bounds)))
    }
  }

  // number of digits required to print our partition values
  private def digits(bits: Int): Int = math.ceil(bits * math.log10(2)).toInt
}

object Z2Scheme extends SpatialPartitionSchemeFactory[Point]("z2") {
  override def buildPartitionScheme(bits: Int, geom: String, geomIndex: Int): PartitionScheme =
    Z2Scheme(geom, geomIndex, bits)
}
