/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geohash

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, Point, PrecisionModel}

import scala.collection.BitSet
import scala.collection.immutable.{BitSet => IBitSet}

/**
 * GeoHashes above GeoHash.MAX_PRECISION are not supported.
 * @param x
 * @param y
 * @param bbox
 * @param bitset
 * @param prec
 */
case class GeoHash private(x: Double,
                           y: Double,
                           bbox: BoundingBox,
                           bitset: BitSet,
                           prec: Int, // checked in factory methods in companion object
                           private val optHash: Option[String]) extends Comparable[GeoHash] {
  require(x >= -180.0 && x <= 180.0)
  require(y >= -90.0  && y <= 90.0)

  import org.locationtech.geomesa.utils.geohash.GeoHash._

  /**
   * Hash string is calculated lazily if GeoHash object was created
   * from a Point, because calculation is expensive
   */
  lazy val hash = optHash.getOrElse(toBase32(bitset, prec))

  lazy val geom = bbox.geom

  /**
   * Utility method to return the bit-string as a full binary string.
   *
   * NB:  We align our bits to the RIGHT by default, not to the LEFT.
   * This means that the most significant bit in the GeoHash (as it is
   * interpreted to a geometry) is actually the least significant bit
   * in the bit-set.  In other words:  Interpret the bit-set from
   * right to left.
   *
   * For clarity (?), this routine prints out the bits in MSB -> LSB
   * order, so that their correspondence with the base-32
   * characters is directly readable.
   */
  def toBinaryString: String =
    (0 until prec).map(bitIndex => boolMap(bitset(bitIndex))).mkString

  /**
    * Converts the geohash into an int.
    *
    * Note: geohashes with more than 6 base32 digits will not fit in an int
    *
    * @return the geohash as an int
    */
  def toInt: Int = {
    var i = 0
    var asInt = 0
    while (i < prec) {
      if (bitset(i)) {
        asInt += 1 << prec - 1 - i
      }
      i += 1
    }
    asInt
  }

  def getPoint = GeoHash.factory.createPoint(new Coordinate(x,y))

  def contains(gh: GeoHash): Boolean = prec <= gh.prec && bitset.subsetOf(gh.bitset)

  def next(): GeoHash =  GeoHash(GeoHash.next(bitset, prec), prec)

  override def equals(obj: Any): Boolean = obj match {
    case that: GeoHash => this.bitset == that.bitset && this.prec == that.prec
    case _ => false
  }

  // Overriding equals obligates us to override hashCode.
  override def hashCode: Int = bitset.hashCode + prec

  override def compareTo(gh: GeoHash) = this.hash.compareTo(gh.hash)
}

case class Bounds(low: Double,
                  high: Double) {
  lazy val mid = (low+high)/2.0
}

object GeoHash extends LazyLogging {

  val MAX_PRECISION = 63 // our bitset operations assume all bits fit in one Long

  implicit def toGeometry(gh: GeoHash) = gh.geom

  private val boolMap : Map[Boolean,String] = Map(false -> "0", true -> "1")

  lazy val factory: GeometryFactory = new GeometryFactory(new PrecisionModel, 4326)

  private val latBounds = Bounds(-90.0,90.0)
  private lazy val latRange: Double = latBounds.high - latBounds.low
  private val lonBounds = Bounds(-180.0,180.0)
  private lazy val lonRange: Double = lonBounds.high - lonBounds.low

  private lazy val powersOf2Map: Map[Int, Long] =
    (0 to   MAX_PRECISION).map(i => (i, 1L << i)).toMap // 1L << i == math.pow(2,i).toLong

  private lazy val latDeltaMap: Map[Int, Double]  =
    (0 to MAX_PRECISION).map(i => (i, latRange / powersOf2Map(i / 2))).toMap
  def latitudeDeltaForPrecision(prec: Int): Double = {
    checkPrecision(prec)
    latDeltaMap(prec)
  }

  private lazy val lonDeltaMap: Map[Int, Double] =
    (0 to MAX_PRECISION).map(i => (i, lonRange / powersOf2Map(i / 2 + i % 2))).toMap
  def longitudeDeltaForPrecision(prec: Int): Double = {
    checkPrecision(prec)
    lonDeltaMap(prec)
  }

  private val bits = Array(16,8,4,2,1)
  protected[geohash] val base32 = "0123456789bcdefghjkmnpqrstuvwxyz"
  private val characterMap: Map[Char, BitSet] =
    base32.zipWithIndex.map { case (c, i) => c -> bitSetFromBase32Character(i) }.toMap

  private val lonMax = 360 - math.pow(0.5, 32)
  private val latMax = 180 - math.pow(0.5, 32)

  // create a new GeoHash from a binary string in MSB -> LSB format
  // (analogous to what is output by the "toBinaryString" method)
  def fromBinaryString(bitsString: String): GeoHash = {
    val numBits = bitsString.length
    val bitSet: BitSet = bitsString.zipWithIndex.foldLeft(BitSet())((bsSoFar, bitPosTuple) => bitPosTuple match {
      case (bitChar, pos) if bitChar == '1' => bsSoFar + pos
      case _                                => bsSoFar
    })
    apply(bitSet, numBits)
  }

  def apply(string: String): GeoHash = decode(string) // precision checked in decode
  def apply(string: String, precision:Int): GeoHash = decode(string, Some[Int](precision)) // precision checked in decode

  // We expect points in x,y order, i.e., longitude first.
  def apply(p: Point, prec: Int): GeoHash = apply(p.getX, p.getY, prec) // precision checked in apply
  def apply(bs: BitSet, prec: Int): GeoHash = decode(toBase32(bs, prec), Some(prec)) // precision checked in decode

  // We expect points x,y i.e., lon-lat
  def apply(lon: Double, lat: Double, prec: Int = 25): GeoHash = {
    require(lon >= -180.0 && lon <= 180.0)
    require(lat >= -90.0  && lat <= 90.0)
    checkPrecision(prec)

    val lonDelta = lonDeltaMap(prec)
    val latDelta = latDeltaMap(prec)
    val lonIndex = if(lon == 180.0) (lonMax / lonDelta).toLong else ((lon - lonBounds.low) / lonDelta).toLong
    val latIndex = if(lat == 90.0)  (latMax / latDelta).toLong else ((lat - latBounds.low) / latDelta).toLong

    encode(lonIndex, latIndex, lonDelta, latDelta, prec)
  }

  def covering(ll: GeoHash, ur: GeoHash, prec: Int = 25) = {
    checkPrecision(prec)

    val bbox = BoundingBox(ll.getPoint, ur.getPoint)

    def subsIntersecting(hash: GeoHash): Seq[GeoHash] = {
      if (hash.prec == prec) List(hash)
      else {
        val subs = BoundingBox.generateSubGeoHashes(hash)
        subs.flatMap { case sub =>
          if (!bbox.intersects(sub.bbox)) List()
          else subsIntersecting(sub)
        }
      }
    }
    val init = BoundingBox.getCoveringGeoHash(bbox, prec)
    subsIntersecting(init)
  }

  def checkPrecision(precision: Int) =
    require(precision <= MAX_PRECISION,
            s"GeoHash precision of $precision requested, but precisions above $MAX_PRECISION are not supported")

  /**
   * Get the dimensions of the geohash grid bounded by ll and ur at precision.
   * @param g1
   * @param g2
   * @param precision
   * @return tuple containing (latitude span count, longitude span count)
   */
  def getLatitudeLongitudeSpanCount(g1: GeoHash, g2: GeoHash, precision: Int): (Int, Int) = {
    require(g1.prec == precision,
            s"Geohash ${g1.hash} has precision ${g1.prec} but precision ${precision} is required")
    require(g2.prec == precision,
            s"Geohash ${g2.hash} has precision ${g2.prec} but precision ${precision} is required")

    val Array(latIndex1, lonIndex1) = GeoHash.gridIndicesForLatLong(g1)
    val Array(latIndex2, lonIndex2) = GeoHash.gridIndicesForLatLong(g2)

    ((math.abs(latIndex2 - latIndex1) + 1).toInt, (math.abs(lonIndex2 - lonIndex1) + 1).toInt)
  }

  /**
   * Convenience method to return both the latitude and longitude indices within
   * the grid of geohashes at the precision of the specified geohash
   *
   * @param gh the geohash
   * @return an array containing first the latitude index and then the longitude index
   */
  def gridIndicesForLatLong(gh: GeoHash) = {
    val (lonIndex, latIndex) = extractReverseBits(gh.bitset.toBitMask.head, gh.prec)
    Array(latIndex, lonIndex)
  }

  /**
   * Gets a long value representing a latitude index within the grid of geohashes
   * at the precision of the specified geohash
   *
   * @param gh the geohash
   * @return latitude index
   */
  def gridIndexForLatitude(gh: GeoHash) = {
    val (_, latIndex) = extractReverseBits(gh.bitset.toBitMask.head, gh.prec)
    latIndex
  }


  /**
   * Gets a long value representing a longitude index within th grid of geohashes
   * at the precision of the specified geohash
   *
   * @param gh the geohash
   * @return longitude index
   */
  def gridIndexForLongitude(gh: GeoHash) = {
    val (lonIndex, _) = extractReverseBits(gh.bitset.toBitMask.head, gh.prec)
    lonIndex
  }


  /**
   * Composes a geohash from a latitude and longitude index for the grid of geohashes
   * at the specified precision.
   *
   * Note that the maximum latitude index is 2^(prec / 2) - 1 and the maximum longitude index
   * is 2^(prec / 2 + prec % 2) -1 for the given precision.  An exception will be thrown if a
   * larger index value is passed to this function.
   *
   * @param latIndex latitude index
   * @param lonIndex longitude index
   * @param prec the precision
   * @return a geohash at the specified latitude and longitude index for the given geohash precision
   */
  def composeGeoHashFromBitIndicesAndPrec(latIndex: Long, lonIndex: Long, prec: Int): GeoHash = {
    checkPrecision(prec)
    encode(lonIndex, latIndex, lonDeltaMap(prec), latDeltaMap(prec), prec)
  }


  def next(gh: GeoHash): GeoHash = GeoHash(GeoHash.next(gh.bitset), gh.prec)

  def next(bs:BitSet, precision:Int=63) : BitSet = {
    (0 until precision).reverse.foldLeft(true,BitSet())((t,idx) => t match { case (carry,newBS) => {
      if (carry) {
        if (bs(idx)) (true, newBS)
        else (false, newBS+idx)
      } else {
        if (bs(idx)) (false, newBS+idx)
        else (false, newBS)
      }
    }})._2
  }


  /**
   * Create the geohash at the given latitude and longitude index with the given deltas and precision.
   * Assumes prec <= 63, that is, all bits in the bitset fit in one Long
   * @param lonIndex: the longitude index (x index)
   * @param latIndex: the latitude index (y index)
   * @param lonDelta the longitude (x) delta per grid cell for the given precision
   * @param latDelta the latitude (y) delta per grid cell for the given precision
   * @param prec precision (# of bits)
   * @return the encoded GeoHash (note that calculation of the actual hash string is lazy)
   */
  def encode(lonIndex: Long, latIndex: Long, lonDelta: Double, latDelta: Double, prec: Int): GeoHash = {

    val bitSet = IBitSet.fromBitMaskNoCopy(Array(interleaveReverseBits(lonIndex, latIndex, prec)))
    val bbox = bboxFromIndiciesDeltas(lonIndex, latIndex, lonDelta, latDelta)

    GeoHash(bbox.midLon, bbox.midLat, bbox, bitSet, prec, None)
  }

  /**
   * There is no visible difference between "t4bt" as a 20-bit GeoHash and
   * the same string as a 17-bit GeoHash.  Unless otherwise specified, assume
   * that the string represents a full complement of bits.
   *
   * If the call specifies a precision, then there is some additional work:
   * 1.  the full-characters (those each representing a full 5 bits) should
   *     be interpreted as they were before
   * 2.  the partial-character (zero or one; representing the remainder bits,
   *     guaranteed to be fewer than 5) must be appended
   *
   * @param string the base-32 encoded string to decode
   * @param precisionOption the desired precision as an option
   * @return the decoded GeoHash
   */
  private def decode(string: String, precisionOption: Option[Int] = None): GeoHash = {
    // figure out what precision we should use
    val prec = precisionOption.getOrElse(5*string.length)
    checkPrecision(prec)

    // compute bit-sets for both the full and partial characters
    val bitsets = string.zipWithIndex.map { case (c: Char, i: Int) => shift(i*5, characterMap(c)) }

    // OR all of these bit-sets together
    val finalBitset: BitSet = bitsets.size match {
      case 0 => BitSet()
      case 1 => bitsets(0)
      case _ => bitsets.reduce(_|_)
    }

    val (lonIndex, latIndex) = extractReverseBits(finalBitset.toBitMask.head, prec)
    val (lonDelta, latDelta) = (lonDeltaMap(prec), latDeltaMap(prec))
    val bbox = bboxFromIndiciesDeltas(lonIndex, latIndex, lonDelta, latDelta)

    GeoHash(bbox.midLon, bbox.midLat, bbox, finalBitset, prec, Some(string))
  }

  private def bboxFromIndiciesDeltas(lonIndex: Long,
                                     latIndex: Long,
                                     lonDelta: Double,
                                     latDelta: Double): BoundingBox =
    BoundingBox(Bounds((lonBounds.low+lonDelta*lonIndex), (lonBounds.low+lonDelta*(lonIndex+1))),
                Bounds((latBounds.low+latDelta*latIndex), (latBounds.low+latDelta*(latIndex+1))))

  /**
   * Reverses and interleaves every other bits of two longs into one long. The two longs must be
   * same size or first can be one bit longer than second.
   * @param first first long
   * @param second second long
   * @param numBits The total number of bits of the interleaved & reversed result
   * @return long with a total of numBits bits of first and second interleaved & reversed
   */
  private def interleaveReverseBits(first: Long, second: Long, numBits: Int): Long = {
    /* We start with the first value of the interleaved long, coming from first if
       numBits is odd or from second if numBits is even */
    val even = (numBits & 0x01) == 0
    val (actualFirst, actualSecond) = if(even) (second, first) else (first, second)
    val numPairs = numBits >> 1
    var result = 0L
    (0 until numPairs).foreach{ pairNum =>
      result = (result << 1) | ((actualFirst >> pairNum) & 1L)
      result = (result << 1) | ((actualSecond >> pairNum) & 1L)
    }
    if (!even) (result << 1) | ((actualFirst >> numPairs) & 1L) else result
  }

  /**
   * Reverses and extracts every other bit up to numBits and extracts them into two longs.
   * @param value the long
   * @param numBits the number of bits to extract from the long
   * @return (first long, second long)  The first long will be one bit longer than the second if
   *         numBits is odd
   */
  private def extractReverseBits(value: Long, numBits: Int): (Long, Long) = {
    val numPairs = numBits >> 1
    var first = 0L
    var second = 0L
    (0 until numPairs).foreach{ pairNum =>
      first = (first << 1) | ((value >> pairNum * 2) & 1L)
      second = (second << 1) | ((value >> (pairNum * 2 + 1)) & 1L)
    }
    ((if((numBits & 0x01) == 1) ((first << 1) | ((value >> (numBits - 1)) & 1L)) else first), second)
  }

  private def shift(n: Int, bs: BitSet): BitSet = bs.map(_ + n)

  private def bitSetFromBase32Character(charIndex: Long): BitSet =
    BitSet(toPaddedBinaryString(charIndex, 5).zipWithIndex.filter { case (c,idx) => c == '1' }.map(_._2): _*)

  private def toPaddedBinaryString(i: Long, length: Int): String =
    String.format("%" + length + "s", i.toBinaryString).replace(' ', '0')

  private def toBase32(bitset: BitSet, prec: Int): String = {
    // compute the precision padded to the next 5-bit boundary
    val numLeftoverBits = prec % 5
    val precision = prec + (numLeftoverBits match {
      case 0 => 0
      case _ => (5-numLeftoverBits)
    })

    // take the bit positions in groups of 5, and map each set to a character
    // (based on the current bit-set); this works for off-5 precisions, because
    // the additional bits will simply not be there (assumed to be zero)
    (0 until precision).grouped(5).map(i=>ch(i, bitset)).mkString
  }

  private def ch(v: IndexedSeq[Int], bitset: BitSet) =
    base32(v.foldLeft(0)((cur,i) => cur + (if (bitset(i)) bits(i%bits.length) else 0)))

}
