/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.curve

import com.google.common.geometry._
import org.locationtech.sfcurve.IndexRange

/**
  * S2 space-filling curve
  */
class S2SFC(minLevel: Int, maxLevel: Int, levelMod: Int, maxCells: Int) extends SpaceFillingCurve {

  import scala.collection.JavaConverters._

  import S2SFC.{LatMax, LatMin, LonMax, LonMin}

  override def index(x: Double, y: Double, lenient: Boolean): Long = {
    if (lenient) {
      val bx = if (x < LonMin) { LonMin } else if (x > LonMax) { LonMax } else { x }
      val by = if (y < LatMin) { LatMin } else if (y > LatMax) { LatMax } else { y }
      S2CellId.fromLatLng(S2LatLng.fromDegrees(by, bx)).id()
    } else {
      require(x >= LonMin && x <= LonMax && y >= LatMin && y <= LatMax,
        s"Value(s) out of bounds ([$LonMin,$LonMax], [$LatMin,$LatMax]): $x, $y")
      S2CellId.fromLatLng(S2LatLng.fromDegrees(y, x)).id()
    }
  }

  override def ranges(
      xy: Seq[(Double, Double, Double, Double)],
      precision: Int,
      maxRanges: Option[Int]): Seq[IndexRange] = {

    xy.flatMap { case (xmin, ymin, xmax, ymax) =>
      val lo = S2LatLng.fromDegrees(ymin, xmin)
      val hi = S2LatLng.fromDegrees(ymax, xmax)
      val rect = new S2LatLngRect(lo, hi)

      val cover = new S2RegionCoverer()
      cover.setMinLevel(minLevel)
      cover.setMaxLevel(maxLevel)
      cover.setLevelMod(levelMod)
      cover.setMaxCells(maxCells)

      val s2CellUnion = cover.getCovering(rect)

      val builder = Seq.newBuilder[IndexRange]
      builder.sizeHint(s2CellUnion.cellIds().size())
      s2CellUnion.cellIds().asScala.foreach(c => builder += IndexRange(c.rangeMin().id(), c.rangeMax().id(), contained = true))
      builder.result()
    }
  }

  override def invert(i: Long): (Double, Double) = {
    val latLon = new S2CellId(i).toLatLng
    (latLon.lngDegrees(), latLon.latDegrees())
  }
}

object S2SFC {

  private val LonMin: Double = -180d
  private val LonMax: Double = 180d
  private val LatMin: Double = -90d
  private val LatMax: Double = 90d

  def apply(minLevel: Int, maxLevel: Int, levelMod: Int, maxCells: Int): S2SFC =
    new S2SFC(minLevel, maxLevel, levelMod, maxCells)
}

