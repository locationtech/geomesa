/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.curve

import com.google.common.geometry._

import scala.collection.JavaConversions._

/**
  * s2 space-filling curve
  *
  * @author sunyabo 2019年07月26日 09:11
  * @version V1.0
  */
class S2SFC(minLevel: Int, maxLevel: Int, levelMod: Int, maxCells: Int) extends S2SpaceFillingCurve[S2CellId] {

  private val lonMin: Double = -180d
  private val latMin: Double = -90d
  private val lonMax: Double = 180d
  private val latMax: Double = 90d

  override def index(x: Double, y: Double, lenient: Boolean): S2CellId = {
    try {
      require(x >= lonMin && x <= lonMax && y >= latMin && y <= latMax,
        s"Value(s) out of bounds ([$lonMin,$lonMax], [$latMin,$latMax]): $x, $y")
      S2CellId.fromLatLng(S2LatLng.fromDegrees(y, x))
    } catch {
      case _: IllegalArgumentException if lenient => lenientIndex(x, y)
    }
  }

  protected def lenientIndex(x: Double, y: Double): S2CellId = {
    val bx = if (x < lonMin) { lonMin } else if (x > lonMax) { lonMax } else { x }
    val by = if (y < latMin) { latMin } else if (y > latMax) { latMax } else { y }
    S2CellId.fromLatLng(S2LatLng.fromDegrees(by, bx))
  }

  /**
    * get s2 cell union as ranges
    * @return
    */
  override def ranges(xy: Seq [(Double, Double, Double, Double)],
                      maxRanges: Option[Int]): Seq[S2CellId] = {

    val rect = new S2LatLngRect(S2LatLng.fromDegrees(xy.head._2, xy.head._1),
      S2LatLng.fromDegrees(xy.head._4, xy.head._3))

    val cover: S2RegionCoverer = new S2RegionCoverer
    cover.setMinLevel(minLevel)
    cover.setMaxLevel(maxLevel)
    cover.setLevelMod(levelMod)
    cover.setMaxCells(maxCells)

    val s2CellUnion = cover.getCovering(rect)

    s2CellUnion.cellIds().toSeq
  }
}

object S2SFC {
  def apply(minLevel: Int, maxLevel: Int, levelMod: Int, maxCells: Int): S2SFC =
    new S2SFC(minLevel, maxLevel, levelMod, maxCells)
}

