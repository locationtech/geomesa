/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geomesa.utils.geohash

import com.vividsolutions.jts.geom.{Point, Coordinate, PrecisionModel, GeometryFactory}
import scala.collection.BitSet
import scala.collection.immutable.{BitSet => IBitSet}
import com.typesafe.scalalogging.slf4j.Logging

/**
 * GeoHashes above GeoHash.MAX_PRECISION are not supported.
 * @param x
 * @param y
 * @param bbox
 * @param bitset
 * @param prec
 * @param specifiedHash
 */
case class GeoHash(x: Double,
                   y: Double,
                   bbox: BoundingBox,
                   bitset: BitSet,
                   prec: Int,
                   specifiedHash: Option[String]) extends Comparable[GeoHash] {

  import GeoHash._

  /**
   * Hash string is calculated lazily if GeoHash object was created
   * from a Point, because calculation is expensive
   */
  lazy val hash = specifiedHash.getOrElse(toBase32(bitset, prec))

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
    (0 until prec).map((bitIndex) => boolMap(bitset(bitIndex))).mkString

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

object GeoHash extends Logging {

  val MAX_PRECISION = 50
  private[GeoHash] val boolMap : Map[Boolean,String] = Map(false -> "0", true -> "1")
  lazy val factory: GeometryFactory = new GeometryFactory(new PrecisionModel, 4326)

  def apply(string: String): GeoHash = decode(string)
  def apply(string: String, precision:Int): GeoHash = decode(string, Some[Int](precision))

  // We expect points in x,y order, i.e., longitude first.
  def apply(p: Point, prec: Int): GeoHash = apply(p.getX, p.getY, prec)
  def apply(bs: BitSet, prec: Int): GeoHash = decode(toBase32(bs, prec), Some(prec))

  // We expect points x,y i.e., lon-lat
  def apply(lon: Double, lat: Double, prec: Int = 25): GeoHash = {
    val (bbox, bitset) = boxBitsForLonLatPrec(lon, lat, prec)
    GeoHash(bbox.midLon, bbox.midLat, bbox, bitset, prec, None)
  }

  def covering(ll: GeoHash, ur: GeoHash, prec: Int = 25) = {
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
  def gridIndicesForLatLong(gh: GeoHash) = Array(gridIndexForLatitude(gh),
                                                 gridIndexForLongitude(gh))

  /**
   * Gets a long value representing a latitude index within the grid of geohashes
   * at the precision of the specified geohash
   *
   * @param gh the geohash
   * @return latitude index
   */
  def gridIndexForLatitude(gh: GeoHash) =
    bitSetToIndex(extractEveryOtherBitFromBitset(gh.bitset, 1, gh.prec), gh.prec / 2)

  /**
   * Gets a long value representing a longitude index within th grid of geohashes
   * at the precision of the specified geohash
   *
   * @param gh the geohash
   * @return longitude index
   */
  def gridIndexForLongitude(gh: GeoHash) =
    bitSetToIndex(extractEveryOtherBitFromBitset(gh.bitset, 0, gh.prec), gh.prec / 2 + gh.prec % 2)

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
    val bitsLat = prec / 2
    val bitsLon = bitsLat + prec % 2

    val latBinString = toPaddedBinaryString(latIndex, bitsLat)
    val lonBinString = toPaddedBinaryString(lonIndex, bitsLon)

    require(latBinString.length == bitsLat, "latitude Long value " + latIndex +" too high for precision of " + prec)
    require(lonBinString.length == bitsLon, "longitude Long value " + lonIndex +" too high for precision of " + prec)

    val bs = BitSet((0 until prec).map(i => if (i % 2 == 0) lonBinString(i / 2) else latBinString(i / 2))
      .zipWithIndex.filter { case (c, idx) => c == '1' }.map(_._2): _*)

    GeoHash(bs, prec)
  }

  def next(gh: GeoHash): GeoHash = GeoHash(GeoHash.next(gh.bitset), gh.prec)

  def next(bs:BitSet, precision:Int=64) : BitSet = {
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

  private val bits = Array(16,8,4,2,1)
  private val latBounds = Bounds(-90.0,90.0)
  private lazy val latRange: Double = latBounds.high - latBounds.low
  private val lonBounds = Bounds(-180.0,180.0)
  private lazy val lonRange: Double = lonBounds.high - lonBounds.low

  private lazy val powersOf2Map: Map[Int, Long] =
    (0 to MAX_PRECISION).map(i => (i, math.pow(2, i).toLong)).toMap
  private lazy val latDeltaMap: Map[Int, Double]  =
    (0 to MAX_PRECISION).map(i => (i, latRange / powersOf2Map(i))).toMap
  private lazy val lonDeltaMap: Map[Int, Double] =
    (0 to MAX_PRECISION).map(i => (i, lonRange / powersOf2Map(i))).toMap

  protected[geohash] val base32 = "0123456789bcdefghjkmnpqrstuvwxyz"
  private val characterMap: Map[Char, BitSet] =
    base32.zipWithIndex.map { case (c, i) => c -> bitSetFromBase32Character(i) }.toMap

  private def bitSetToIndex(bs: BitSet, prec: Int): Long =
    (0 until prec).map(bit => if (bs(bit)) 1 else 0).reverse
      .zipWithIndex.filter{ case (bit, i) => bit > 0 }.map{ case (bit, i) => math.pow(2,i).toLong }.sum

  private def extractEveryOtherBitFromBitset(bs: BitSet, offset:Int, prec:Int): BitSet =
    BitSet((offset until prec by 2).map(bit => if (bs(bit)) 1 else 0)
      .zipWithIndex.filter { case (bit, i) => bit > 0}.map(_._2): _*)

  private def bitSetFromBase32Character(charIndex: Long): BitSet =
    BitSet(toPaddedBinaryString(charIndex, 5).zipWithIndex.filter { case (c,idx) => c == '1' }.map(_._2): _*)

  private def toPaddedBinaryString(i: Long, length: Int): String =
    String.format("%" + length + "s", i.toBinaryString).replace(' ', '0')

  /**
   * Get the bitset and bounding box for a geohash at the given latitude and
   * longitude with the given precision.
   * @param lon the longitude (x value)
   * @param lat the latitude (y value)
   * @param prec precision (# of bits)
   * @return tuple containing the bounding box and bitset.
   */
  private def boxBitsForLonLatPrec(lon: Double, lat: Double, prec: Int): (BoundingBox, BitSet) = {
    val minLon = lonBounds.low
    val minLat = latBounds.low

    val latBits = prec / 2
    val lonBits = latBits + prec % 2

    val lonDelta = lonDeltaMap(lonBits)
    val lonIndex = ((lon - minLon) / lonDelta).toLong

    val latDelta = latDeltaMap(latBits)
    val latIndex = ((lat - minLat) / latDelta).toLong

    val bitSet = IBitSet.fromBitMaskNoCopy(Array(interleaveReverseBits(lonIndex, latIndex, prec)))

    val bbox = BoundingBox(Bounds((minLon+lonDelta*lonIndex), (minLon+lonDelta*(lonIndex+1))),
                           Bounds((minLat+latDelta*latIndex), (minLat+latDelta*(latIndex+1))))

    (bbox, bitSet)
  }

  /**
   * Interleaves and reverses the bits of two longs.
   * The two longs must be same size
   * @param first can be one bit longer than second
   * @param second must be same size as first or one bit shorter
   * @param numBits The total number of bits of the interleaved & reversed result
   * @return long with a total of numBits bits of first and second interleaved & reversed
   */
  private def interleaveReverseBits(first: Long, second: Long, numBits: Int): Long = {
    /* We start with the first value of the interleaved long, coming from first if
     * numBits is odd or from second if numBits is even
     */
    val (actualFirst, actualSecond) = if(numBits % 2 == 0) (second, first) else (first, second)

    (0 until numBits).foldLeft(0L){ case (currLong, i) =>
      val indIndex = i / 2
      if(i % 2 == 0) currLong | shiftLongLeft(actualFirst & (1L << indIndex), numBits-3*indIndex-1)
      else currLong | shiftLongLeft(actualSecond & (1L << indIndex), numBits-3*indIndex-2)
    }
  }

  /**
   * Shifts a long to the left if value is positive, or right if negative. 0 does not shift.
   * @param value
   * @param shift
   * @return
   */
  private def shiftLongLeft(value: Long, shift: Int) = if(shift > 0) value << shift else value >> -shift

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
  private def decode(string: String, precisionOption:Option[Int]=None): GeoHash = {
    // figure out what precision we should use
    val precision : Int = precisionOption.getOrElse(5*string.length)

    // compute bit-sets for both the full and partial characters
    val bitsets : Seq[BitSet] = string.zipWithIndex.map {
      case (c: Char, i: Int) => shift(i*5, characterMap(c))
    }

    // OR all of these bit-sets together
    val finalBitset : BitSet = bitsets.size match {
      case 0 => BitSet()
      case 1 => bitsets(0)
      case _ => bitsets.reduce(_|_)
    }

    // compute the geometry implied by this bit-set
    val lonb = lonFromBitset(finalBitset, precision)
    val latb = latFromBitset(finalBitset, precision)
    val bbox = BoundingBox(lonb, latb)

    GeoHash(lonb.mid, latb.mid, bbox, finalBitset, precision, Some(string))
  }

  private def lonFromBitset = boundsFromBitset(0, lonBounds)(_,_)
  private def latFromBitset = boundsFromBitset(1, latBounds)(_,_)
  private def boundsFromBitset(startIdx: Int, bounds: Bounds)(bs: BitSet, prec: Int): Bounds =
    (startIdx until prec by 2).foldLeft(bounds) {
      case (bounds: Bounds, i: Int) =>
        if(!bs(i)) bounds.copy(high=bounds.mid)
        else bounds.copy(low=bounds.mid)
    }


  private def shift(n: Int, bs: BitSet): BitSet = bs.map(_ + n)

  private def toBase32(bitset: BitSet, prec: Int): String = {
    // compute the precision padded to the next 5-bit boundary
    val numLeftoverBits = prec % 5
    val precision : Int = prec + (numLeftoverBits match {
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

  //@todo make faster?
  def subHashes(geohash:GeoHash)=
    base32.map(str=>GeoHash(geohash.hash+str))

}
