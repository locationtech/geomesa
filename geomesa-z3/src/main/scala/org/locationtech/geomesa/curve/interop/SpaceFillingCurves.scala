/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.curve.interop

import java.util.{List => jList}

import org.locationtech.geomesa.curve.SpaceFillingCurve.FullPrecision
import org.locationtech.geomesa.curve.Z2SFC.{lat, lon}
import org.locationtech.geomesa.curve.{XZ2SFC, Z2SFC}

import scala.collection.JavaConverters._

/**
 * Java interop-friendly facade over the Z2SFC and XZ2SFC space-filling curves.
 *
 * Inputs are clamped to the WGS84 domain, matching the curves' lenient
 * handling of out-of-bounds coordinates.
 *
 * Hex outputs delegate to `Z2SFC.hexEncode`/`XZ2SFC.hexEncode`, and
 * left-align the significant bits so lexicographic (truncate-prefix)
 * comparison over the encoded strings matches numeric comparison of the
 * underlying index values.
 */
object SpaceFillingCurves {

  /**
   * A contiguous, inclusive range of hex-encoded curve values —
   * Java-interop stand-in for Scala's `IndexRange`.
   *
   * @param lower inclusive lower bound
   * @param upper inclusive upper bound
   */
  case class HexRange(lower: String, upper: String)

  private def clampLon(x: Double): Double = math.min(lon.max, math.max(lon.min, x))
  private def clampLat(y: Double): Double = math.min(lat.max, math.max(lat.min, y))

  /**
   * Z2 cell index for a single (lon, lat) point at the default Z2SFC precision
   * (31 bits/axis, 62-bit positive Long).
   *
   * @param lon longitude, clamped to [-180, 180]
   * @param lat latitude, clamped to [-90, 90]
   * @return the Z2SFC index value
   */
  def z2Index(lon: Double, lat: Double): Long =
    Z2SFC.index(clampLon(lon), clampLat(lat))

  /**
   * Z2 cell value for a single (lon, lat) point, hex-encoded via
   * `Z2SFC.hexEncode` (left-shifted).
   *
   * @param lon longitude, clamped to [-180, 180]
   * @param lat latitude, clamped to [-90, 90]
   * @return the hex-encoded Z2SFC index value
   */
  def z2Hex(lon: Double, lat: Double): String =
    Z2SFC.hexEncode(z2Index(lon, lat))

  /**
   * Z2 index ranges covering the provided lat/lon bounds, hex-encoded via
   * `Z2SFC.hexEncode`. Returns inclusive `HexRange` pairs;
   * stored values produced by `z2Hex` that compare lexicographically.
   *
   * @param minLon envelope min longitude, clamped to [-180, 180]
   * @param minLat envelope min latitude, clamped to [-90, 90]
   * @param maxLon envelope max longitude, clamped to [-180, 180]
   * @param maxLat envelope max latitude, clamped to [-90, 90]
   * @param maxRanges rough upper bound on the number of ranges returned; the SFC
   *                  coarsens (merges) past it, so the cover remains a superset of
   *                  the envelope — pruning gets less selective, never lossy
   * @return inclusive hex range pairs
   */
  def z2HexRanges(minLon: Double, minLat: Double, maxLon: Double, maxLat: Double, maxRanges: Int): jList[HexRange] =
    Z2SFC.ranges((clampLon(minLon), clampLon(maxLon)), (clampLat(minLat), clampLat(maxLat)), FullPrecision, Some(maxRanges)).iterator
      .map(r => HexRange(Z2SFC.hexEncode(r.lower), Z2SFC.hexEncode(r.upper)))
      .toList.asJava

  /**
   * XZ2 cell index for lat/lon bounds at the given `g` resolution.
   * Returns the appropriate XZ2SFC sequence-code.
   *
   * @param minLon envelope min longitude, clamped to [-180, 180]
   * @param minLat envelope min latitude, clamped to [-90, 90]
   * @param maxLon envelope max longitude, clamped to [-180, 180]
   * @param maxLat envelope max latitude, clamped to [-90, 90]
   * @param g XZ2 quad-tree resolution
   * @return the XZ2SFC sequence-code value
   */
  def xz2Index(minLon: Double, minLat: Double, maxLon: Double, maxLat: Double, g: Short): Long =
    XZ2SFC(g).index(clampLon(minLon), clampLat(minLat), clampLon(maxLon), clampLat(maxLat))

  /**
   * XZ2 cell value for a geometry's envelope at the given `g` resolution,
   * hex-encoded via `XZ2SFC.hexEncode` (bit-shifted left so the significant
   * bits are left-aligned and truncate-prefix matching works).
   *
   * @param minLon envelope min longitude, clamped to [-180, 180]
   * @param minLat envelope min latitude, clamped to [-90, 90]
   * @param maxLon envelope max longitude, clamped to [-180, 180]
   * @param maxLat envelope max latitude, clamped to [-90, 90]
   * @param g XZ2 quad-tree resolution
   * @return the hex-encoded XZ2SFC sequence-code value
   */
  def xz2Hex(minLon: Double, minLat: Double, maxLon: Double, maxLat: Double, g: Short): String =
    XZ2SFC(g).hexEncode(xz2Index(minLon, minLat, maxLon, maxLat, g))

  /**
   * XZ2 index ranges covering the query envelope at the given `g` resolution,
   * hex-encoded via `XZ2SFC.hexEncode`. Returns inclusive `HexRange` pairs;
   * stored values produced by `xz2Hex` compare lexicographically within them.
   *
   * @param minLon envelope min longitude, clamped to [-180, 180]
   * @param minLat envelope min latitude, clamped to [-90, 90]
   * @param maxLon envelope max longitude, clamped to [-180, 180]
   * @param maxLat envelope max latitude, clamped to [-90, 90]
   * @param g XZ2 quad-tree resolution
   * @param maxRanges rough upper bound on the number of ranges returned; the SFC
   *                  coarsens (merges) past it, so the cover remains a superset of
   *                  the envelope — pruning gets less selective, never lossy
   * @return inclusive hex range pairs
   */
  def xz2HexRanges(minLon: Double, minLat: Double, maxLon: Double, maxLat: Double, g: Short, maxRanges: Int): jList[HexRange] = {
    val sfc = XZ2SFC(g)
    sfc.ranges((clampLon(minLon), clampLat(minLat), clampLon(maxLon), clampLat(maxLat)), Some(maxRanges)).iterator
      .map(r => HexRange(sfc.hexEncode(r.lower), sfc.hexEncode(r.upper)))
      .toList.asJava
  }
}
