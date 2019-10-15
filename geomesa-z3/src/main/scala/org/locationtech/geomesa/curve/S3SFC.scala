/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.curve

import com.google.common.geometry._
import org.locationtech.geomesa.curve.TimePeriod.TimePeriod

import scala.collection.JavaConversions._

/**
  * @author sunyabo 2019年08月01日 10:21
  * @version V1.0
  */
class S3SFC(minLevel: Int, maxLevel: Int, levelMod: Int, maxCells: Int, period: TimePeriod) extends S3SpaceFillingCurve[S2CellId] {

  protected val lonMin: Double = -180d
  protected val latMin: Double = -90d
  protected val lonMax: Double = 180d
  protected val latMax: Double = 90d

  override def time: Int = BinnedTime.maxOffset(period).toInt

  val wholePeriod = Seq((0, time))

  override def index(x: Double, y: Double, lenient: Boolean): S2CellId = {
    try {
      require(x >= lonMin && x <= lonMax && y >= latMin && y <= latMax,
        s"Value(s) out of bounds ([$lonMin,$lonMax], [$latMin,$latMax]): $x, $y")
      S2CellId.fromLatLng(S2LatLng.fromDegrees(y, x))
    } catch {
      case _: IllegalArgumentException if lenient => lenientIndex(y, x)
    }
  }

  protected def lenientIndex(x: Double, y: Double): S2CellId = {
    val bx = if (x < lonMin) { lonMin } else if (x > lonMax) { lonMax } else { x }
    val by = if (y < latMin) { latMin } else if (y > latMax) { latMax } else { y }

    S2CellId.fromLatLng(S2LatLng.fromDegrees(by, bx))
  }

  /**
    * Gets google-s2 cell ids as ranges
    *
    * @return
    */
  override def ranges(xy: Seq[(Double, Double, Double, Double)],
                      maxRanges: Option[Int]): Seq[S2CellId] = {
    val startS2 = S2LatLng.fromDegrees(xy.head._2, xy.head._1)
    val endS2 = S2LatLng.fromDegrees(xy.head._4, xy.head._3)
    val rect = new S2LatLngRect(startS2, endS2)

    val cover = new S2RegionCoverer
    cover.setMinLevel(minLevel)
    cover.setMaxLevel(maxLevel)
    cover.setLevelMod(levelMod)
    cover.setMaxCells(maxCells)

    val cellUnion = cover.getCovering(rect)

    cellUnion.cellIds().toSeq
  }
}

object S3SFC {

  def apply(minLevel: Int, maxLevel: Int, levelMod: Int, maxCells: Int, period: TimePeriod): S3SFC = period match {
    case TimePeriod.Day   => new S3SFC(minLevel, maxLevel, levelMod, maxCells, TimePeriod.Day)
    case TimePeriod.Week  => new S3SFC(minLevel, maxLevel, levelMod, maxCells, TimePeriod.Week)
    case TimePeriod.Month => new S3SFC(minLevel, maxLevel, levelMod, maxCells, TimePeriod.Month)
    case TimePeriod.Year  => new S3SFC(minLevel, maxLevel, levelMod, maxCells, TimePeriod.Year)
  }
}

