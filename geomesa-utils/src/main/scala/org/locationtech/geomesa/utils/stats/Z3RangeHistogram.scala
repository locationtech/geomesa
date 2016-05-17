/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.utils.stats

import java.util.Date

import com.vividsolutions.jts.geom.{Coordinate, Geometry, Point}
import org.geotools.geometry.jts.JTSFactoryFinder
import org.joda.time.{DateTime, DateTimeZone, Seconds, Weeks}
import org.locationtech.geomesa.curve.Z3SFC
import org.locationtech.geomesa.utils.stats.MinMax.MinMaxGeometry
import org.locationtech.sfcurve.zorder.Z3
import org.opengis.feature.simple.SimpleFeature

/**
  * The range histogram's state is stored in an indexed array, where the index is the bin number
  * and the values are the counts.
  *
  * Tracks geometry and date attributes as a single value.
  *
  * @param geomIndex geometry attribute index in the sft
  * @param dtgIndex date attribute index in the sft
  * @param length number of bins the histogram has, per week
 */
class Z3RangeHistogram(val geomIndex: Int, val dtgIndex: Int, val length: Int) extends Stat {

  import Z3RangeHistogram._

  override type S = Z3RangeHistogram

  private [stats] val binMap = scala.collection.mutable.Map.empty[Short, BinnedLongArray]
  private [stats] def newBins = new BinnedLongArray(length, (minZ, maxZ))

  def weeks: Seq[Short] = binMap.keys.toSeq.sorted
  def count(week: Short, i: Int): Long = binMap(week).counts(i)

  def directIndex(week: Short, value: Long): Int = binMap.get(week).map(_.indexOf(value)).getOrElse(-1)

  def indexOf(value: (Geometry, Date)): (Short, Int) = {
    val (week, z) = toKey(value._1, value._2)
    (week, directIndex(week, z))
  }

  def medianValue(week: Short, i: Int): (Geometry, Date) = fromKey(week, binMap(week).medianValue(i))

  private def toKey(geom: Geometry, dtg: Date): (Short, Long) = {
    val time = new DateTime(dtg, DateTimeZone.UTC)
    val week = Weeks.weeksBetween(Z3Frequency.Epoch, time).getWeeks
    val secondsInWeek = Seconds.secondsBetween(Z3Frequency.Epoch, time.minusWeeks(week)).getSeconds
    val centroid = geom.getCentroid
    val z = Z3SFC.index(centroid.getX, centroid.getY, secondsInWeek).z
    (week.toShort, z)
  }

  private def fromKey(week: Short, z: Long): (Geometry, Date) = {
    val (x, y, t) = Z3SFC.invert(new Z3(z))
    val dtg = Z3Frequency.Epoch.plusWeeks(week).plusSeconds(t.toInt).toDate
    val geom = Z3RangeHistogram.gf.createPoint(new Coordinate(x, y))
    (geom, dtg)
  }

  /**
    * Split the stat into a separate stat per week of z data. Allows for separate handling of the reduced
    * data set.
    *
    * @return
    */
  def splitByWeek: Seq[(Short, Z3RangeHistogram)] = {
    binMap.toSeq.map { case (w, bins) =>
      val hist = new Z3RangeHistogram(geomIndex, dtgIndex, length)
      hist.binMap.put(w, bins)
      (w, hist)
    }
  }

  override def observe(sf: SimpleFeature): Unit = {
    val geom = sf.getAttribute(geomIndex).asInstanceOf[Geometry]
    val dtg  = sf.getAttribute(dtgIndex).asInstanceOf[Date]
    if (geom != null && dtg != null) {
      val (week, z3) = toKey(geom, dtg)
      binMap.getOrElseUpdate(week, newBins).add(z3, 1L)
    }
  }

  override def unobserve(sf: SimpleFeature): Unit = {
    val geom = sf.getAttribute(geomIndex).asInstanceOf[Geometry]
    val dtg  = sf.getAttribute(dtgIndex).asInstanceOf[Date]
    if (geom != null && dtg != null) {
      val (week, z3) = toKey(geom, dtg)
      binMap.get(week).foreach(_.add(z3, -1L))
    }
  }

  /**
    * Creates a new histogram by combining another histogram with this one
    */
  override def +(other: Z3RangeHistogram): Z3RangeHistogram = {
    val plus = new Z3RangeHistogram(geomIndex, dtgIndex, length)
    plus += this
    plus += other
    plus
  }

  /**
    * Copies another histogram into this one
    */
  override def +=(other: Z3RangeHistogram): Unit = {
    if (length != other.length) {
      throw new NotImplementedError("Can only add z3 histograms with the same length")
    }
    other.binMap.foreach { case (w, obins) =>
      val bins = binMap.getOrElseUpdate(w, newBins)
      var i = 0
      while (i < bins.length) {
        bins.counts(i) += obins.counts(i)
        i += 1
      }
    }
  }

  override def toJson: String = {
    val weeks = binMap.toSeq.sortBy(_._1).map {
      case (w, bins) => f""""week-$w%03d" : { "bins" : [ ${bins.counts.mkString(", ")} ] }"""
    }
    weeks.mkString("{ ", ", ", " }")
  }

  override def isEmpty: Boolean = binMap.values.forall(_.counts.forall(_ == 0))

  override def clear(): Unit = binMap.values.foreach(_.clear())

  override def isEquivalent(other: Stat): Boolean = other match {
    case that: Z3RangeHistogram =>
      geomIndex == that.geomIndex && dtgIndex == that.dtgIndex && length == that.length &&
          binMap.keySet == that.binMap.keySet &&
          binMap.forall { case (w, bins) => java.util.Arrays.equals(bins.counts, that.binMap(w).counts) }
    case _ => false
  }
}

object Z3RangeHistogram {

  val gf = JTSFactoryFinder.getGeometryFactory

  val minGeom = MinMaxGeometry.min.asInstanceOf[Point]
  val maxGeom = MinMaxGeometry.max.asInstanceOf[Point]
  val minDate = Z3SFC.time.min.toLong
  val maxDate = Z3SFC.time.max.toLong
  val minZ = Z3SFC.index(minGeom.getX, minGeom.getY, minDate).z
  val maxZ = Z3SFC.index(maxGeom.getX, maxGeom.getY, maxDate).z

  /**
    * Combines a sequence of split histograms. This will not modify any of the inputs.
    *
    * @param histograms histograms to combine
    * @return
    */
  def combine(histograms: Seq[Z3RangeHistogram]): Option[Z3RangeHistogram] = {
    if (histograms.length < 2) {
      histograms.headOption
    } else {
      // create a new stat so that we don't modify the existing ones
      val summed = histograms.head + histograms.tail.head
      histograms.drop(2).foreach(summed += _)
      Some(summed)
    }
  }
}