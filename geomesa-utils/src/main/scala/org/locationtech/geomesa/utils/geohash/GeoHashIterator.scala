/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geohash

import org.locationtech.jts.geom.{Coordinate, GeometryFactory, Point, PrecisionModel}
import org.locationtech.geomesa.utils.geohash.GeoHashIterator._

import scala.collection.mutable

object GeoHashIterator {
  val geometryFactory = new GeometryFactory(new PrecisionModel, 4326)
  /**
   * Given points, return the two GeoHashes that bound the rectangle that are suitable for
   * iteration.
   *
   * @param points a collection of points for which a minimum-bounding-rectangle (MBR) is sought
   * @param precision the precision, in bits, of the GeoHashes sought
   * @param radiusInMeters the buffer distance in meters
   * @return the lower-left and upper-right corners of the bounding box, as GeoHashes at the specified precision
   */
  def getBoundingGeoHashes(points: Traversable[Point],
                           precision: Int,
                           radiusInMeters: Double): (GeoHash, GeoHash) = {
    val (lonMin, lonMax, latMin, latMax) =
      points.foldLeft((Long.MaxValue, Long.MinValue, Long.MaxValue, Long.MinValue))(
      {case ((xMin, xMax, yMin, yMax), point) => {
        val Array(y, x) = GeoHash.gridIndicesForLatLong(GeoHash.apply(point, precision))
        (math.min(xMin, x), math.max(xMax, x), math.min(yMin, y), math.max(yMax, y))
      }})

    val ptLL = {
      val gh = GeoHash.composeGeoHashFromBitIndicesAndPrec(latMin, lonMin, precision)
      val point = geometryFactory.createPoint(new Coordinate(gh.x, gh.y))
      val left = VincentyModel.moveWithBearingAndDistance(point, -90, radiusInMeters)
      VincentyModel.moveWithBearingAndDistance(left, 180, radiusInMeters)
    }

    val ptUR = {
      val gh = GeoHash.composeGeoHashFromBitIndicesAndPrec(latMax, lonMax, precision)
      val point = geometryFactory.createPoint(new Coordinate(gh.x, gh.y))
      val right = VincentyModel.moveWithBearingAndDistance(point, 0, radiusInMeters)
      VincentyModel.moveWithBearingAndDistance(right, 90, radiusInMeters)
    }

    (GeoHash.apply(ptLL, precision), GeoHash.apply(ptUR, precision))
  }

  /**
   * Longitude ranges over the entire circumference of the Earth, while latitude only ranges over half.
   * Hence, the precision (in meters-per-bit) is twice as refined for latitude as it is for longitude.
   * (The first bit latitude represents ~10,000 Km, while the first bit longitude represents ~20,000 Km.)
   *
   * In addition, latitude spans a slightly smaller range due to the asymmetry of the Earth.
   *
   * @param isLatitude  whether the dimension requested is latitude
   * @param dimensionBits the number of bits used
   * @return how many meters each bit of this dimension represents
   */
  def getDimensionPrecisionInMeters(nearLatitude: Double,
                                    isLatitude: Boolean,
                                    dimensionBits: Int): Double = {
    if (isLatitude) 20004000.0 / (1 << dimensionBits).asInstanceOf[Double]
    else {
      val radiusAtEquator = 40075160.0
      val radiusNearLatitude = radiusAtEquator * Math.cos(nearLatitude * Math.PI / 180.0)
      val circumferenceNearLatitude = radiusNearLatitude * 2.0 * Math.PI
      circumferenceNearLatitude / (1 << dimensionBits).toDouble
    }
  }

  // TODO: none of the utility methods below this point are used within acc-geo; can we delete them?
  /**
   * Given a radius in meters, what is the worst-case size in degrees that it might represent?
   * Note:  This is almost entirely useless as a measure.
   *
   * @param meters a distance in meters
   * @return a distance in degrees
   */
  def convertRadiusInMetersToDegrees(meters: Double): Double = {
    val point = geometryFactory.createPoint(new Coordinate(67.5, 35.0))
    getSegmentLengthInDegrees(point,
                              VincentyModel.moveWithBearingAndDistance(point, 45.0, meters))
  }

  /**
   * Utility storage for converting degrees to meters.
   */
  private final val mapDegreesToMeters = mutable.Map[Double, Double]()
  private final val precisionDegreesToMeters = 1e5

  /**
   * Given a radius in degrees, what is a blended-estimate size in meters that it might represent?
   * Note:  This is almost entirely useless as a measure.
   *
   * @param degreeRadius a distance in degrees
   * @return a distance in meters
   */
  def convertRadiusInDegreesToMeters(degreeRadius: Double): Double = {
    val degrees = Math.round(degreeRadius * precisionDegreesToMeters) / precisionDegreesToMeters
    mapDegreesToMeters.getOrElseUpdate(degrees, convertRadiusInDegreesToMetersViaIntervalHalving(degrees, 45.0))
  }

  /**
   * Given a radius in degrees, what is a blended-estimate size in meters that it might represent?
   * Note:  This is almost entirely useless as a measure.
   *
   * @param degrees a distance in degrees
   * @return a distance in meters
   */
  def convertRadiusInDegreesToMetersViaIntervalHalving(degrees: Double, azimuth: Double): Double = {
    val a = geometryFactory.createPoint(new Coordinate(67.5, 35.0))
    var minMeters = 0.01
    var maxMeters = 10000000.0
    var midMeters = 0.5 * (minMeters + maxMeters)
    var midDegrees = getSegmentLengthInDegrees(a,
                                               VincentyModel.moveWithBearingAndDistance(a, azimuth, midMeters))
    while (Math.abs(midMeters - minMeters) > 0.01) {
      if (midDegrees == degrees) return midMeters
      if (midDegrees > degrees) {
        maxMeters = midMeters
      }
      else if (midDegrees < degrees) {
        minMeters = midMeters
      }
      midMeters = 0.5 * (minMeters + maxMeters)
      midDegrees = getSegmentLengthInDegrees(a,
                                             VincentyModel.moveWithBearingAndDistance(a, azimuth, midMeters))
    }
    midMeters
  }

  /**
   * Utility function to express the distance between two points in degrees.
   * Note:  This is almost entirely useless as a measure.
   *
   * @param a one segment end-point
   * @param b one segment end-point
   * @return the distance in degrees between these two points; note that this can only be
   *         an estimate, as horizontal and vertical degrees do not represent equal distances
   */
  def getSegmentLengthInDegrees(a: Point, b: Point): Double =
    Math.hypot(a.getX - b.getX, a.getY - b.getY)

  /**
   * Utility function to express the distance between two points in meters.
   *
   * @param a one segment end-point
   * @param b one segment end-point
   * @return the distance in meters between these two points
   */
  def getSegmentLengthInMeters(a: Point, b: Point): Double =
    VincentyModel.getDistanceBetweenTwoPoints(a, b).getDistanceInMeters
}


abstract class GeoHashIterator(latitudeLL: Double,
                               longitudeLL: Double,
                               latitudeUR: Double,
                               longitudeUR: Double,
                               precision: Int)
    extends Iterator[GeoHash] {
  private val (llgh, urgh) = {
    val ll = geometryFactory.createPoint(new Coordinate(longitudeLL, latitudeLL))
    val ur = geometryFactory.createPoint(new Coordinate(longitudeUR, latitudeUR))
    getBoundingGeoHashes(Seq(ll, ur), precision, 0.0)
  }

  val Array(latBitsLL, lonBitsLL) = GeoHash.gridIndicesForLatLong(llgh)
  val Array(latBitsUR, lonBitsUR) = GeoHash.gridIndicesForLatLong(urgh)

  protected val midLatitude = 0.5 * llgh.y + urgh.y
  val latPrecision = (precision >> 1)
  val lonPrecision = latPrecision + (precision % 2)
  def latPrecisionInMeters = getDimensionPrecisionInMeters(midLatitude, true, latPrecision)
  def lonPrecisionInMeters = getDimensionPrecisionInMeters(midLatitude, false, lonPrecision)
  val incLatitudeDegrees = 180.0 / Math.pow(2.0, latPrecision)
  val incLongitudeDegrees = 360.0 / Math.pow(2.0, lonPrecision)

  def spanBitsLat: Long = latBitsUR - latBitsLL + 1
  def spanBitsLon: Long = lonBitsUR - lonBitsLL + 1

  // Internal iterator state that IS mutable and IS updated on advance()
  private   var ghCurrent: GeoHash = null
  private   var ghPrevious: GeoHash = null
  protected var doesHaveNext: Boolean = false
  protected var latPosition = latBitsLL
  protected var lonPosition = lonBitsLL
  def currentPoint = ghCurrent.getPoint

  advance()

  /**
   * Internal method that figures out whether the iterator is finished, and if not, updates the
   * current GeoHash and advances the counters.
   *
   * As a general scheme, we start in the lower-left corner, and iterate in a row-major way
   * until we exceed the upper-right corner of the rectangle.
   *
   * @return whether the iteration is over
   */
  protected def advance(): Boolean

  /**
   * Fetch the current result, and advance to the next (at least internally).
   *
   * @return the current GeoHash result
   */
  @Override
  override def next(): GeoHash = {
    val gh = ghCurrent
    doesHaveNext = advance
    gh
  }

  /**
   * Allows the user to query whether there is another GeoHash cell to return.
   *
   * @return whether there is another cell to return
   */
  @Override
  override def hasNext: Boolean = doesHaveNext

  protected def setCurrentGeoHash(newCurrentGeoHash: GeoHash) {
    ghPrevious = ghCurrent
    ghCurrent = newCurrentGeoHash
  }

  protected def getSpanAspectRatioSkew: Double = {
    val nx = spanBitsLon.asInstanceOf[Double]
    val ny = spanBitsLat.asInstanceOf[Double]
    Math.min(nx, ny) / (nx + ny)
  }
}
