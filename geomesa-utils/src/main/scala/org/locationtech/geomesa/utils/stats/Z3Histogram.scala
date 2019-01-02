/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.stats

import java.util.Date

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.jts.geom.{Coordinate, Geometry, Point}
import org.geotools.geometry.jts.JTSFactoryFinder
import org.locationtech.geomesa.curve.TimePeriod.TimePeriod
import org.locationtech.geomesa.curve.{BinnedTime, TimePeriod, Z3SFC}
import org.locationtech.geomesa.utils.stats.MinMax.MinMaxGeometry
import org.locationtech.sfcurve.zorder.Z3
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
  * The histogram's state is stored in an indexed array, where the index is the bin number
  * and the values are the counts.
  *
  * Tracks geometry and date attributes as a single value.
  *
  * @param sft simple feature type
  * @param geom geometry attribute in the sft
  * @param dtg date attribute in the sft
  * @param period time period to use for z index
  * @param length number of bins the histogram has, per period
 */
class Z3Histogram(val sft: SimpleFeatureType,
                  val geom: String,
                  val dtg: String,
                  val period: TimePeriod,
                  val length: Int) extends Stat with LazyLogging {

  import Z3Histogram._

  override type S = Z3Histogram

  @deprecated("geom")
  lazy val geomIndex: Int = g
  @deprecated("dtg")
  lazy val dtgIndex: Int = d

  private val g = sft.indexOf(geom)
  private val d = sft.indexOf(dtg)

  private val sfc = Z3SFC(period)
  private val timeToBin = BinnedTime.timeToBinnedTime(period)
  private val binToDate = BinnedTime.binnedTimeToDate(period)
  private val minZ = sfc.index(minGeom.getX, minGeom.getY, sfc.time.min.toLong).z
  private val maxZ = sfc.index(maxGeom.getX, maxGeom.getY, sfc.time.max.toLong).z

  private lazy val jsonFormat = period match {
    case TimePeriod.Day   => s"$period-%05d"
    case TimePeriod.Week  => s"$period-%04d"
    case TimePeriod.Month => s"$period-%03d"
    case TimePeriod.Year  => s"$period-%02d"
  }

  private [stats] val binMap = scala.collection.mutable.Map.empty[Short, BinnedLongArray]
  private [stats] def newBins = new BinnedLongArray(length, (minZ, maxZ))

  def timeBins: Seq[Short] = binMap.keys.toSeq.sorted
  def count(timeBin: Short, i: Int): Long = binMap.get(timeBin).map(_.counts(i)).getOrElse(0L)

  def directIndex(timeBin: Short, value: Long): Int = binMap.get(timeBin).map(_.indexOf(value)).getOrElse(-1)

  def indexOf(value: (Geometry, Date)): (Short, Int) = {
    val (timeBin, z) = toKey(value._1, value._2, lenient = false)
    (timeBin, directIndex(timeBin, z))
  }

  def medianValue(timeBin: Short, i: Int): (Geometry, Date) = fromKey(timeBin, binMap(timeBin).medianValue(i))

  private def toKey(geom: Geometry, dtg: Date, lenient: Boolean): (Short, Long) = {
    import org.locationtech.geomesa.utils.geotools.Conversions.RichGeometry
    val BinnedTime(bin, offset) = timeToBin(dtg.getTime)
    val centroid = geom.safeCentroid()
    val z = sfc.index(centroid.getX, centroid.getY, offset, lenient).z
    (bin, z)
  }

  private def fromKey(timeBin: Short, z: Long): (Geometry, Date) = {
    val (x, y, t) = sfc.invert(new Z3(z))
    val dtg = Date.from(binToDate(BinnedTime(timeBin, t)).toInstant)
    val geom = Z3Histogram.gf.createPoint(new Coordinate(x, y))
    (geom, dtg)
  }

  /**
    * Split the stat into a separate stat per time bin of z data. Allows for separate handling of the reduced
    * data set.
    *
    * @return
    */
  def splitByTime: Seq[(Short, Z3Histogram)] = {
    binMap.toSeq.map { case (w, bins) =>
      val hist = new Z3Histogram(sft, geom, dtg, period, length)
      hist.binMap.put(w, bins)
      (w, hist)
    }
  }

  override def observe(sf: SimpleFeature): Unit = {
    val geom = sf.getAttribute(g).asInstanceOf[Geometry]
    val dtg  = sf.getAttribute(d).asInstanceOf[Date]
    if (geom != null && dtg != null) {
      try {
        val (timeBin, z3) = toKey(geom, dtg, lenient = false)
        binMap.getOrElseUpdate(timeBin, newBins).add(z3, 1L)
      } catch {
        case e: Exception => logger.warn(s"Error observing geom '$geom' and date '$dtg': ${e.toString}")
      }
    }
  }

  override def unobserve(sf: SimpleFeature): Unit = {
    val geom = sf.getAttribute(g).asInstanceOf[Geometry]
    val dtg  = sf.getAttribute(d).asInstanceOf[Date]
    if (geom != null && dtg != null) {
      try {
        val (timeBin, z3) = toKey(geom, dtg, lenient = true)
        binMap.get(timeBin).foreach(_.add(z3, -1L))
      } catch {
        case e: Exception => logger.warn(s"Error un-observing geom '$geom' and date '$dtg': ${e.toString}")
      }
    }
  }

  /**
    * Creates a new histogram by combining another histogram with this one
    */
  override def +(other: Z3Histogram): Z3Histogram = {
    val plus = new Z3Histogram(sft, geom, dtg, period, length)
    plus += this
    plus += other
    plus
  }

  /**
    * Copies another histogram into this one
    */
  override def +=(other: Z3Histogram): Unit = {
    if (length != other.length) {
      throw new NotImplementedError("Can only add z3 histograms with the same length")
    }
    other.binMap.foreach { case (w, bins) =>
      binMap.get(w) match {
        case None => binMap.put(w, bins) // note: sharing a reference now
        case Some(b) =>
          var i = 0
          while (i < b.length) {
            b.counts(i) += bins.counts(i)
            i += 1
          }
      }
    }
  }

  override def toJsonObject: Any =
    binMap.toSeq.sortBy(_._1)
      .map { case (p, bins) => (String.format(jsonFormat, Short.box(p)), bins) }
      .map { case (label, bins) => Map(label-> Map("bins" -> bins.counts)) }

  override def isEmpty: Boolean = binMap.values.forall(_.counts.forall(_ == 0))

  override def clear(): Unit = binMap.values.foreach(_.clear())

  override def isEquivalent(other: Stat): Boolean = other match {
    case that: Z3Histogram =>
      g == that.g && d == that.d && period == that.period &&
          length == that.length && binMap.keySet == that.binMap.keySet &&
          binMap.forall { case (w, bins) => java.util.Arrays.equals(bins.counts, that.binMap(w).counts) }
    case _ => false
  }
}

object Z3Histogram {

  private val gf = JTSFactoryFinder.getGeometryFactory

  val minGeom: Point = MinMaxGeometry.min.asInstanceOf[Point]
  val maxGeom: Point = MinMaxGeometry.max.asInstanceOf[Point]
}