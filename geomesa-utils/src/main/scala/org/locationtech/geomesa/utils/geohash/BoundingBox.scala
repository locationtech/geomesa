/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.utils.geohash

import org.locationtech.jts.geom._
import org.apache.commons.codec.binary.Base64
import org.locationtech.geomesa.utils.text.WKBUtils

import scala.collection.BitSet

/**
 *   A bounding box is a lat-lon rectangle defined by a "lower left"
 *    and an "upper right" point.
 *
 *   We do make the assumption that the lower left corner's latitude and longitude are less
 *   than or equal to the upper right corner's.
 *
 *   NB: Lat-lon rectangles which cross the {+|-}180 longitude line cannot be represented.
 */
case class BoundingBox(ll: Point, ur: Point) {
  
  import org.locationtech.geomesa.utils.geohash.BoundingBox._

  require(ll.getX <= ur.getX, s"Bounding box lower left X: ${ll.getX} > upper right X: ${ur.getX}")
  require(ll.getY <= ur.getY, s"Bounding box lower left Y: ${ll.getY} > upper right Y: ${ur.getY}")

  lazy val envelope: Envelope = new Envelope(ll.getX, ur.getX, ll.getY, ur.getY)
  lazy val geom: Geometry = latLonGeoFactory.toGeometry(envelope)

  lazy val ul: Point = latLonGeoFactory.createPoint(new Coordinate(ll.getX, ur.getY))
  lazy val lr: Point = latLonGeoFactory.createPoint(new Coordinate(ur.getX, ll.getY))

  def intersects(bbox: BoundingBox): Boolean = covers(bbox.ll) || covers(bbox.ul) ||
                                               covers(bbox.lr) || covers(bbox.ur)

  /**
   * This bounding box covers bbox iff no points of bbox lie in the exterior of this bounding box.
   * @param bbox
   * @return
   */
  def covers(bbox: BoundingBox):Boolean = covers(bbox.ll) && covers(bbox.ul) &&
                                          covers(bbox.lr) && covers(bbox.ur)

  /**
   * This bounding box covers pt iff pt does not line in the exterior of this bounding box.
   * @param pt
   * @return
   */
  def covers(pt: Point): Boolean = (ll.getX <= pt.getX && pt.getX <= ur.getX) &&
                                   (ll.getY <= pt.getY && pt.getY <= ur.getY)

  /**
   * This bounding box contains geom iff no points of goem lie in the exterior of this bounding box,
   * and at least one point of the interior of goem lies in the interior of this bounding box.
   * @param otherGeom
   * @return
   */
  def contains(otherGeom: Geometry): Boolean = geom.contains(otherGeom)

  lazy val longitudeSize = ur.getX - ll.getX
  lazy val latitudeSize = ur.getY - ll.getY

  lazy val minLon = ll.getX
  lazy val minLat = ll.getY
  lazy val maxLon = ur.getX
  lazy val maxLat = ur.getY

  // these very simple calculations work because this class assumes min{x|y} < max{x|y}
  // (no date line or pole wrap-arounds)
  lazy val midLon = (minLon + maxLon) / 2
  lazy val midLat = (minLat + maxLat) / 2

  lazy val centerPoint = latLonGeoFactory.createPoint(new Coordinate(midLon,midLat))

  def getExpandedBoundingBox(that: BoundingBox): BoundingBox =
    BoundingBox.getCoveringBoundingBox(this, that)
}

object BoundingBox {
  /* TODO: remove this and refer to max precision in GeoHash class? */
  val DEFAULT_PRECISION = 40

  implicit def toEnvelope(bbox: BoundingBox): Envelope = bbox.envelope
  implicit def toGeometry(bbox: BoundingBox): Geometry = bbox.geom

  lazy val latLonGeoFactory = new GeometryFactory(new PrecisionModel(PrecisionModel.FLOATING), 4326)

  def apply(x1: Double, x2: Double, y1: Double, y2: Double): BoundingBox =
     apply(Bounds(Math.min(x1, x2), Math.max(x1, x2)),
           Bounds(Math.min(y1, y2), Math.max(y1, y2)))

  def apply(lons: Bounds, lats: Bounds): BoundingBox = {
    val Bounds(minLat, maxLat) = lats
    val Bounds(minLon, maxLon) = lons
    new BoundingBox(
      latLonGeoFactory.createPoint(new Coordinate(minLon, minLat)),
      latLonGeoFactory.createPoint(new Coordinate(maxLon, maxLat)))
  }

  def apply(env: Envelope): BoundingBox = {
    apply(env.getMinX, env.getMaxX, env.getMinY, env.getMaxY)
  }

  def geoHashFromEwkb(ewkb: String, precision: Int = DEFAULT_PRECISION): GeoHash = {
    val b = Base64.decodeBase64(ewkb.getBytes)
    val wkt = WKBUtils.read(b)
    GeoHash(wkt.getInteriorPoint.getY, wkt.getInteriorPoint.getX, precision)
  }

  def getAreaOfBoundingBox(bbox: BoundingBox): Double = bbox.geom.getArea

  def bboxToPoly(ll: Point, ur: Point): Polygon =
    latLonGeoFactory.createPolygon(
      latLonGeoFactory.createLinearRing(Array(new Coordinate(ll.getX, ll.getY),
        new Coordinate(ll.getX, ur.getY),
        new Coordinate(ur.getX, ur.getY),
        new Coordinate(ur.getX, ll.getY),
        new Coordinate(ll.getX, ll.getY))),
      Array())

  def intersects(l: BoundingBox, r: BoundingBox): Boolean = l.geom.intersects(r.geom)

  def getCoveringBoundingBox(l:BoundingBox, r:BoundingBox) = {
    val maxLon = math.max(l.ur.getX, r.ur.getX)
    val minLon = math.min(l.ll.getX, r.ll.getX)
    val maxLat = math.max(l.ur.getY, r.ur.getY)
    val minLat = math.min( l.ll.getY,r.ll.getY)
    BoundingBox(Bounds(minLon,maxLon), Bounds(minLat,maxLat))
  }

  /**
   * Here only to get around inexplicable errors (perhaps caused by old version of java - 6u26):
   * Caused by: java.lang.NoSuchMethodError: geomesa.utils.geohash.BoundingBox$.getGeoHashesFromBoundingBox$default$2()I
   *     at geomesa.plugin.wms.CoverageReader.getScanBuffers(CoverageReader.scala:140)
   *     at geomesa.plugin.wms.CoverageReader.getImage(CoverageReader.scala:126)
   *     at geomesa.plugin.wms.CoverageReader.read(CoverageReader.scala:108)
   * I suspect a scala bug, or a bug in the way we're packaging geomesa plugin
   *
   * @param bbox
   * @return
   */
  def getGeoHashesFromBoundingBox(bbox: BoundingBox): List[String] =
    getGeoHashesFromBoundingBox(bbox, 32, DEFAULT_PRECISION)

  /**
   *
   * @param bbox
   * @param maxHashes default is 32 - not sure if 32 is optimal, but seems to work well
   * @return
   */
  def getGeoHashesFromBoundingBox(bbox: BoundingBox,
                                  maxHashes: Int = 32,
                                  precision: Int = DEFAULT_PRECISION): List[String] = {

    def getMinBoxes(hashList: List[GeoHash]): List[String] = {
      val hashes = hashList.flatMap(h => generateSubGeoHashes(h)) filter
          (hash => intersects(bbox, hash.bbox))
      if (hashes.size < maxHashes && hashes.size > 0 && hashes.head.prec < precision) {
        //double check - you could get way too many here from the subhashing
        val childHashes = getMinBoxes(hashes)
        if (childHashes.size > maxHashes) {
          hashes.map(hash => hash.hash)
        } else {
          childHashes
        }
      } else {
        hashes.map(hash => hash.hash)
      }
    }
    getMinBoxes(List(getCoveringGeoHash(bbox, precision)))
  }

  /**
   * This method is *only* here for Java compatibility
   * @param bbox
   * @param maxHashes
   * @return
   */
  def getCoveringGeoHashesFromBoundingBox(bbox: BoundingBox,
                                          maxHashes: Int): CoveringGeoHashes =
    getCoveringGeoHashesFromBoundingBox(bbox, maxHashes, DEFAULT_PRECISION)

  def getCoveringGeoHashesFromBoundingBox(bbox: BoundingBox,
                                          maxHashes: Int,
                                          precision: Int = DEFAULT_PRECISION): CoveringGeoHashes =
    new CoveringGeoHashes(getGeoHashesFromBoundingBox(bbox, maxHashes, precision).map(GeoHash(_)))

  /**
   * get geohash that covers the bounding box to the given precision 
   * @param bbox
   * @param precision
   * @return
   */
  def getCoveringGeoHash(bbox: BoundingBox, precision: Int) = {
    val ll = GeoHash(bbox.ll, precision).hash
    val ur = GeoHash(bbox.ur.getX-1e-12, bbox.ur.getY-1e-12, precision).hash
    GeoHash(ll.zip(ur).takeWhile(Function.tupled(_ == _)).unzip._1.mkString)
  }

  def generateSubGeoHashes(hash: GeoHash): Seq[GeoHash] =
    (0 to 31).map { i =>
      val oneBits = padLongToString(i)
                    .zipWithIndex
                    .filter { case (ch, idx) => ch != '0' }
                    .map { case (ch, idx) => hash.prec + idx }
      GeoHash(hash.bitset | BitSet(oneBits: _*), hash.prec + 5)
    }

  private def padLongToString(i: Long) = String.format("%5s", i.toBinaryString).replace(' ', '0')
}

class CoveringGeoHashes(ghs: List[GeoHash]) {
  def contains(gh: GeoHash): Boolean = ghs.map(_.contains(gh)).reduce(_|_)
}
