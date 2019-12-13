/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.jts.util

import org.locationtech.jts.geom.{Envelope, Geometry, GeometryFactory, PrecisionModel}
import scala.collection.BitSet
import scala.collection.immutable.{BitSet => IBitSet}
import java.{lang => jl}

/**
  * Exposes the two methods needed by ST_GeoHash and ST_GeomFromGeoHash
  * to convert between geometries and Geohashes.
  * This is different from GeoHashUtils in geomesa-utils which provides operations
  * on geohashes not required by these UDFs
  */
object GeoHashUtils {

  def encode(geom: Geometry, precision: Int): String = {
    val centroid = geom.getCentroid
    val safeCentroid = if (jl.Double.isNaN(centroid.getCoordinate.x) || jl.Double.isNaN(centroid.getCoordinate.y)) {
      geom.getEnvelope.getCentroid
    } else {
      centroid
    }
    GeoHash.encode(safeCentroid.getX, safeCentroid.getY, precision)
  }

  def decode(geohash: String, precision: Int): Geometry = {
    GeoHash.decode(geohash, Some(precision))
  }
}

case class Bounds(low: Double,
                  high: Double) {
  lazy val mid = (low+high)/2.0
}

// This object is composed of minimal pieces from org.locationtech.geomesa.utils.geohash.GeoHash
// to support geohash UDFs while avoiding depending on that module.
private[util] object GeoHash {

  val MAX_PRECISION = 63 // our bitset operations assume all bits fit in one Long

  private val latBounds = Bounds(-90.0,90.0)
  private val lonBounds = Bounds(-180.0,180.0)
  private lazy val latRange: Double = latBounds.high - latBounds.low
  private lazy val lonRange: Double = lonBounds.high - lonBounds.low

  private lazy val powersOf2Map: Map[Int, Long] =
    (0 to   MAX_PRECISION).map(i => (i, 1L << i)).toMap // 1L << i == math.pow(2,i).toLong
  private lazy val latDeltaMap: Map[Int, Double]  =
    (0 to MAX_PRECISION).map(i => (i, latRange / powersOf2Map(i / 2))).toMap
  private lazy val lonDeltaMap: Map[Int, Double] =
    (0 to MAX_PRECISION).map(i => (i, lonRange / powersOf2Map(i / 2 + i % 2))).toMap

  def latitudeDeltaForPrecision(prec: Int): Double = {
    checkPrecision(prec)
    latDeltaMap(prec)
  }

  def longitudeDeltaForPrecision(prec: Int): Double = {
    checkPrecision(prec)
    lonDeltaMap(prec)
  }

  private val lonMax = 360 - math.pow(0.5, 32)
  private val latMax = 180 - math.pow(0.5, 32)

  private val bits = Array(16,8,4,2,1)
  protected[util] val base32 = "0123456789bcdefghjkmnpqrstuvwxyz"
  private val characterMap: Map[Char, BitSet] =
    base32.zipWithIndex.map { case (c, i) => c -> bitSetFromBase32Character(i) }.toMap

  lazy val latLonGeoFactory = new GeometryFactory(new PrecisionModel(PrecisionModel.FLOATING), 4326)

  private def envFromIndiciesDeltas(lonIndex: Long,
                                    latIndex: Long,
                                    lonDelta: Double,
                                    latDelta: Double): Envelope =
    new Envelope(lonBounds.low+lonDelta*lonIndex, lonBounds.low+lonDelta*(lonIndex+1),
      latBounds.low+latDelta*latIndex, latBounds.low+latDelta*(latIndex+1))

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

  def checkPrecision(precision: Int) =
    require(precision <= MAX_PRECISION,
      s"GeoHash precision of $precision requested, but precisions above $MAX_PRECISION are not supported")

  def encode(lon: Double, lat: Double, prec: Int = 25): String = {
    require(lon >= -180.0 && lon <= 180.0)
    require(lat >= -90.0 && lat <= 90.0)
    checkPrecision(prec)

    val lonDelta = lonDeltaMap(prec)
    val latDelta = latDeltaMap(prec)
    val lonIndex = if (lon == 180.0) (lonMax / lonDelta).toLong else ((lon - lonBounds.low) / lonDelta).toLong
    val latIndex = if (lat == 90.0) (latMax / latDelta).toLong else ((lat - latBounds.low) / latDelta).toLong

    val bitSet = IBitSet.fromBitMaskNoCopy(Array(interleaveReverseBits(lonIndex, latIndex, prec)))
    toBase32(bitSet, prec)
  }

  def decode(string: String, precisionOption: Option[Int] = None): Geometry = {
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

    val envelope = envFromIndiciesDeltas(lonIndex, latIndex, lonDelta, latDelta)

    latLonGeoFactory.toGeometry(envelope)
  }

}