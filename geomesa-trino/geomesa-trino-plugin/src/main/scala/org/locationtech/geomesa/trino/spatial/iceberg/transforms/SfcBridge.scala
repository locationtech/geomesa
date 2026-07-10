/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg.transforms

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
 * Hex outputs delegate to `Z2SFC.hexEncode`/`XZ2SFC.hexEncode`, which
 * left-align the significant bits so lexicographic (truncate-prefix)
 * comparison over the encoded strings matches numeric comparison of the
 * underlying index values — the same encoding GeoMesa writers use for
 * hex-partitioned storage columns.
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
   * Z2 cell value for a single (lon, lat) point, hex-encoded via
   * `Z2SFC.hexEncode` (left-aligned so truncate-prefix matching works).
   */
  def z2Hex(lon: Double, lat: Double): String =
    Z2SFC.hexEncode(z2Index(lon, lat))

  /**
   * Z2 index ranges covering the query envelope, hex-encoded via
   * `Z2SFC.hexEncode`. Returns an array of inclusive `[lower, upper]` pairs;
   * stored values produced by `z2Hex` compare lexicographically within them.
   *
   * @param maxRanges rough upper bound on the number of ranges returned; the SFC
   *                  coarsens (merges) past it, so the cover remains a superset of
   *                  the envelope — pruning gets less selective, never lossy
   */
  def z2HexRanges(xMin: Double, yMin: Double, xMax: Double, yMax: Double, maxRanges: Int): Array[Array[String]] =
    Z2SFC.ranges((clampLon(xMin), clampLon(xMax)), (clampLat(yMin), clampLat(yMax)), 64, Some(maxRanges)).iterator
      .map(r => Array(Z2SFC.hexEncode(r.lower), Z2SFC.hexEncode(r.upper)))
      .toArray

  /**
   * XZ2 cell index for a geometry's envelope at the given `g` resolution.
   * Returns the sequence-code Long produced by XZ2SFC(g).index.
   */
  def xz2Index(xMin: Double, yMin: Double, xMax: Double, yMax: Double, g: Short): Long =
    XZ2SFC(g).index(clampLon(xMin), clampLat(yMin), clampLon(xMax), clampLat(yMax), lenient = false)

  /**
   * XZ2 cell value for a geometry's envelope at the given `g` resolution,
   * hex-encoded via `XZ2SFC.hexEncode` (bit-shifted left so the significant
   * bits are left-aligned and truncate-prefix matching works).
   */
  def xz2Hex(xMin: Double, yMin: Double, xMax: Double, yMax: Double, g: Short): String =
    XZ2SFC(g).hexEncode(xz2Index(xMin, yMin, xMax, yMax, g))

  /**
   * XZ2 index ranges covering the query envelope at the given `g` resolution,
   * hex-encoded via `XZ2SFC.hexEncode`. Returns an array of inclusive
   * `[lower, upper]` pairs; stored values produced by `xz2Hex` compare
   * lexicographically within them.
   *
   * @param maxRanges rough upper bound on the number of ranges returned; the SFC
   *                  coarsens (merges) past it, so the cover remains a superset of
   *                  the envelope — pruning gets less selective, never lossy
   */
  def xz2HexRanges(xMin: Double, yMin: Double, xMax: Double, yMax: Double, g: Short, maxRanges: Int): Array[Array[String]] = {
    val sfc = XZ2SFC(g)
    sfc.ranges((clampLon(xMin), clampLat(yMin), clampLon(xMax), clampLat(yMax)), Some(maxRanges)).iterator
      .map(r => Array(sfc.hexEncode(r.lower), sfc.hexEncode(r.upper)))
      .toArray
  }
}
