/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geohash

import org.locationtech.jts.geom.Geometry


/**
 * <p/>
 * Iterates over the GeoHashes at a fixed resolution within a given rectangle.  The difference
 * between this iterator and the default iteration available through GeoHash is that this
 * iterator considers only those GeoHashes known to be inside the bounding rectangle.
 * <p/>
 * The GeoHash is a combination of hashes of its two dimensions, latitude and longitude; the
 * total precision of the GeoHash is defined as a fixed number of bits.
 * <p/>
 * Each bit represents an interval-halving decision.  The first longitude bit, then, is interpreted as
 * follows:  a 0 means that the target longitude is on the interval [0, 180); a 1 means its on [180, 360].
 * The first two bits together define a cell that is 90 degrees wide; the first three bits together define
 * a cell that is 45 degrees wide, etc.
 * <p/>
 * The bit-strings from the two dimensions are interleaved (they alternate), beginning with longitude,
 * so a GeoHash of 10110 (read Lon-Lat-Lon-Lat-Lon) consists of three bits of longitude (110) and two bits of latitude (01).
 * The following is an example of how the GeoHashes (at 5-bits precision) progress:
 * <p/>
 * Longitude
 * 000      001     010      011       100      101     110      111
 * -----   -----   -----   -----    -----   -----   -----   -----
 * 11 |  01010  01011  01110  01111  11010  11011  11110  11111
 * 10 |  01000  01001  01100  01101  11000  11001  11100  11101
 * 01 |  00010  00011  00110  00111  10010  10011  10110  10111
 * 00 |  00000  00001  00100  00101  10000  10001  10100  10101
 * <p/>
 * Each cell in this example is 45 degrees wide and 45 degrees high (since longitude ranges over [0,360],
 * and latitude only goes from [-90,90]).
 * <p/>
 * Note that the dimension-specific bit-strings proceed in order (longitude from 0 to 7; latitude from 0 to 3)
 * along each axis.  That allows us to work on these bit-strings as coordinate indexes, making it simple to
 * iterate over the GeoHashes within a rectangle (and make some estimates about in-circle membership).
 */
object RectangleGeoHashIterator {
  /**
   * Offset, in degrees, by which the LL and UR corners are perturbed
   * to make sure they don't fall on GeoHash boundaries (that may be
   * shared between GeoHashes).
   */
  val OFFSET_DEGREES = 1e-6

  // alternate constructor
  def apply(geometry: Geometry, precision: Int) = {
    val env = geometry.getEnvelopeInternal
    new RectangleGeoHashIterator(env.getMinY, env.getMinX, env.getMaxY, env.getMaxX, precision)
  }
}

import org.locationtech.geomesa.utils.geohash.RectangleGeoHashIterator._

class RectangleGeoHashIterator(latitudeLL: Double,
                               longitudeLL: Double,
                               latitudeUR: Double,
                               longitudeUR: Double,
                               precision: Int)
    extends GeoHashIterator(latitudeLL + OFFSET_DEGREES,
                            longitudeLL + OFFSET_DEGREES,
                            latitudeUR - OFFSET_DEGREES,
                            longitudeUR - OFFSET_DEGREES,
                            precision) {

  /**
   * Internal method that figures out whether the iterator is finished, and if not, updates the
   * current GeoHash and advances the counters.
   * <p/>
   * As a general scheme, we start in the lower-left corner, and iterate in a row-major way
   * until we exceed the upper-right corner of the rectangle.
   *
   * @return whether the iteration is over
   */
  @Override
  override protected def advance: Boolean = {
    doesHaveNext = true
    if (latPosition > latBitsUR) {
      setCurrentGeoHash(null)
      doesHaveNext = false
      return false
    }
    setCurrentGeoHash(GeoHash.composeGeoHashFromBitIndicesAndPrec(latPosition, lonPosition, precision))
    lonPosition +=1
    if (lonPosition > lonBitsUR) {
      latPosition += 1
      lonPosition = lonBitsLL
    }
    true
  }
}
