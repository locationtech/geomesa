/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.utils.stats

import java.util.Date

import com.clearspring.analytics.stream.frequency.{CountMinSketch, RichCountMinSketch}
import com.vividsolutions.jts.geom.Geometry
import org.joda.time.{DateTime, DateTimeZone, Seconds, Weeks}
import org.locationtech.geomesa.curve.Z3SFC
import org.opengis.feature.simple.SimpleFeature

/**
  * Estimates frequency counts at scale. Tracks geometry and date attributes as a single value.
  *
  * @param geomIndex geometry attribute index in the sft
  * @param dtgIndex date attribute index in the sft
  * @param precision number of bits of z-index that will be used
  * @param eps (epsilon) with probability at least @see confidence, estimates will be within eps * N
  * @param confidence percent - with probability at least confidence, estimates will be within @see eps * N
  */
class Z3Frequency(val geomIndex: Int,
                  val dtgIndex: Int,
                  val precision: Int,
                  val eps: Double = 0.005,
                  val confidence: Double = 0.95) extends Stat {

  override type S = Z3Frequency

  private val mask = Frequency.getMask(precision)

  private [stats] val sketches = scala.collection.mutable.Map.empty[Short, CountMinSketch]
  private [stats] def newSketch: CountMinSketch = new CountMinSketch(eps, confidence, Frequency.Seed)

  private def toKey(geom: Geometry, dtg: Date): (Short, Long) = {
    val time = new DateTime(dtg, DateTimeZone.UTC)
    val week = Weeks.weeksBetween(Z3Frequency.Epoch, time).getWeeks
    val secondsInWeek = Seconds.secondsBetween(Z3Frequency.Epoch, time.minusWeeks(week)).getSeconds
    val centroid = geom.getCentroid
    val z = Z3SFC.index(centroid.getX, centroid.getY, secondsInWeek).z & mask
    (week.toShort, z)
  }

  /**
    * Gets the count for the given values
    *
    * @param geom geometry
    * @param dtg date
    * @return count of the values
    */
  def count(geom: Geometry, dtg: Date): Long = {
    val (week, z3) = toKey(geom, dtg)
    countDirect(week, z3)
  }

  /**
    * Gets the count for a week and z3. Useful if the values are known ahead of time.
    *
    * @param week week since the epoch
    * @param z3 z value
    * @return count of the values
    */
  def countDirect(week: Short, z3: Long): Long = sketches.get(week).map(_.estimateCount(z3)).getOrElse(0L)

  /**
    * Number of observations in the frequency map
    *
    * @return number of observations
    */
  def size: Long = sketches.values.map(_.size()).sum

  /**
    * Split the stat into a separate stat per week of z data. Allows for separate handling of the reduced
    * data set.
    *
    * @return
    */
  def splitByWeek: Seq[(Short, Z3Frequency)] = {
    sketches.toSeq.map { case (w, sketch) =>
      val freq = new Z3Frequency(geomIndex, dtgIndex, precision, eps, confidence)
      freq.sketches.put(w, sketch)
      (w, freq)
    }
  }

  override def observe(sf: SimpleFeature): Unit = {
    val geom = sf.getAttribute(geomIndex).asInstanceOf[Geometry]
    val dtg  = sf.getAttribute(dtgIndex).asInstanceOf[Date]
    if (geom != null && dtg != null) {
      val (week, z3) = toKey(geom, dtg)
      sketches.getOrElseUpdate(week, newSketch).add(z3, 1L)
    }
  }

  // no-op
  override def unobserve(sf: SimpleFeature): Unit = {}

  override def +(other: Z3Frequency): Z3Frequency = {
    val plus = new Z3Frequency(geomIndex, dtgIndex, precision, eps, confidence)
    plus += this
    plus += other
    plus
  }

  override def +=(other: Z3Frequency): Unit = {
    other.sketches.filter(_._2.size > 0).foreach { case (w, sketch) =>
      new RichCountMinSketch(sketches.getOrElseUpdate(w, newSketch)).add(sketch)
    }
  }

  override def clear(): Unit = sketches.values.foreach(sketch => new RichCountMinSketch(sketch).clear())

  override def isEmpty: Boolean = sketches.values.forall(_.size == 0)

  override def toJson: String = {
    val sketch = sketches.values.headOption.map(new RichCountMinSketch(_))
    val (w, d) = sketch.map(s => (s.width, s.depth)).getOrElse((0, 0))
    s"{ width : $w, depth : $d, size : $size }"
  }

  override def isEquivalent(other: Stat): Boolean = {
    other match {
      case s: Z3Frequency =>
        geomIndex == s.geomIndex && dtgIndex == s.dtgIndex && precision == s.precision && {
          val nonEmpty = sketches.filter(_._2.size() > 0)
          val sNonEmpty = s.sketches.filter(_._2.size() > 0)
          nonEmpty.keys == sNonEmpty.keys && nonEmpty.keys.forall { k =>
            new RichCountMinSketch(sketches(k)).isEquivalent(s.sketches(k))
          }
        }
      case _ => false
    }
  }
}

object Z3Frequency {

  val Epoch = new DateTime(0, DateTimeZone.UTC)

  /**
    * Combines a sequence of split frequencies. This will not modify any of the inputs.
    *
    * @param frequencies frequencies to combine
    * @return
    */
  def combine(frequencies: Seq[Z3Frequency]): Option[Z3Frequency] = {
    if (frequencies.length < 2) {
      frequencies.headOption
    } else {
      // create a new stat so that we don't modify the existing ones
      val summed = frequencies.head + frequencies.tail.head
      frequencies.drop(2).foreach(summed += _)
      Some(summed)
    }
  }
}
