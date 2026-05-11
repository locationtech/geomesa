/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.iceberg.spatial.transforms

import org.locationtech.geomesa.curve.{XZ2SFC, Z2SFC}

/**
 * Java-friendly facade over GeoMesa's Z2SFC and XZ2SFC space-filling curves.
 *
 * Java code cannot reference Scala's package-object types whose binary class
 * names start with the `package` keyword (`package$IndexRange`, `package$ZRange`
 * in `org.locationtech.geomesa.zorder.sfcurve`) — that's a JLS reserved-word
 * conflict. This bridge keeps all such references on the Scala side and
 * returns plain `Array[Array[Long]]` to Java callers.
 *
 * Bridge methods take no Scala default arguments; all parameters are explicit.
 *
 * Outputs match the cloud GeoMesa writer bit-for-bit:
 *   - Z2: non-negative 62-bit Long (31 bits/axis).
 *   - XZ2 at g=12: sequence code in [0, ~22M].
 */
object SfcBridge {
  private def clampLon(x: Double): Double = math.min(180.0, math.max(-180.0, x))
  private def clampLat(y: Double): Double = math.min(90.0, math.max(-90.0, y))

  /**
   * Z2 cell index for a single (lon, lat) point at the default Z2SFC precision
   * (31 bits/axis, 62-bit positive Long).
   */
  def z2Index(lon: Double, lat: Double): Long =
    Z2SFC.index(clampLon(lon), clampLat(lat), lenient = false)

  /**
   * Z2 index ranges covering the query envelope. Returns an array of
   * `[lower, upper]` pairs in Z2SFC's native (non-negative) Long space.
   *
   * @param maxRanges rough upper bound on the number of ranges returned; the SFC
   *                  coarsens (merges) past it, so the cover remains a superset of
   *                  the envelope — pruning gets less selective, never lossy
   */
  def z2RangesAsLongs(xMin: Double, yMin: Double, xMax: Double, yMax: Double, maxRanges: Int): Array[Array[Long]] =
    Z2SFC.ranges((clampLon(xMin), clampLon(xMax)), (clampLat(yMin), clampLat(yMax)), 64, Some(maxRanges)).iterator
      .map(r => Array(r.lower, r.upper))
      .toArray

  /**
   * XZ2 cell index for a geometry's envelope at the given `g` resolution.
   * Returns the sequence-code Long produced by XZ2SFC(g).index.
   */
  def xz2Index(xMin: Double, yMin: Double, xMax: Double, yMax: Double, g: Short): Long =
    XZ2SFC(g).index(clampLon(xMin), clampLat(yMin), clampLon(xMax), clampLat(yMax), lenient = false)

  /**
   * XZ2 index ranges covering the query envelope at the given `g` resolution.
   * Returns an array of `[lower, upper]` pairs in XZ2SFC sequence-code Long space.
   *
   * @param maxRanges rough upper bound on the number of ranges returned; the SFC
   *                  coarsens (merges) past it, so the cover remains a superset of
   *                  the envelope — pruning gets less selective, never lossy
   */
  def xz2RangesAsLongs(xMin: Double, yMin: Double, xMax: Double, yMax: Double, g: Short, maxRanges: Int): Array[Array[Long]] =
    XZ2SFC(g).ranges((clampLon(xMin), clampLat(yMin), clampLon(xMax), clampLat(yMax)), Some(maxRanges)).iterator
      .map(r => Array(r.lower, r.upper))
      .toArray
}
