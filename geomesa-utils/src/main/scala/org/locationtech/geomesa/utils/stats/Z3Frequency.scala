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
import org.locationtech.jts.geom.Geometry
import org.locationtech.geomesa.curve.TimePeriod.TimePeriod
import org.locationtech.geomesa.curve.{BinnedTime, Z3SFC}
import org.locationtech.geomesa.utils.clearspring.CountMinSketch
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.immutable.ListMap

/**
  * Estimates frequency counts at scale. Tracks geometry and date attributes as a single value.
  *
  * @param sft simple feature type
  * @param geom geometry attribute in the sft
  * @param dtg date attribute in the sft
  * @param period time period to use for z index
  * @param precision number of bits of z-index that will be used
  * @param eps (epsilon) with probability at least @see confidence, estimates will be within eps * N
  * @param confidence percent - with probability at least confidence, estimates will be within @see eps * N
  */
class Z3Frequency(val sft: SimpleFeatureType,
                  val geom: String,
                  val dtg: String,
                  val period: TimePeriod,
                  val precision: Int,
                  val eps: Double = 0.005,
                  val confidence: Double = 0.95) extends Stat with LazyLogging {

  override type S = Z3Frequency

  @deprecated("geom")
  lazy val geomIndex: Int = g
  @deprecated("dtg")
  lazy val dtgIndex: Int = d

  private val g = sft.indexOf(geom)
  private val d = sft.indexOf(dtg)

  private val mask = Frequency.getMask(precision)
  private val sfc = Z3SFC(period)
  private val timeToBin = BinnedTime.timeToBinnedTime(period)

  private [stats] val sketches = scala.collection.mutable.Map.empty[Short, CountMinSketch]
  private [stats] def newSketch: CountMinSketch = CountMinSketch(eps, confidence, Frequency.Seed)

  private def toKey(geom: Geometry, dtg: Date): (Short, Long) = {
    import org.locationtech.geomesa.utils.geotools.Conversions.RichGeometry
    val BinnedTime(b, o) = timeToBin(dtg.getTime)
    val centroid = geom.safeCentroid()
    val z = sfc.index(centroid.getX, centroid.getY, o).z & mask
    (b, z)
  }

  /**
    * Gets the count for the given values
    *
    * @param geom geometry
    * @param dtg date
    * @return count of the values
    */
  def count(geom: Geometry, dtg: Date): Long = {
    val (bin, z3) = toKey(geom, dtg)
    countDirect(bin, z3)
  }

  /**
    * Gets the count for a time bin and z3. Useful if the values are known ahead of time.
    *
    * @param bin period since the epoch
    * @param z3 z value
    * @return count of the values
    */
  def countDirect(bin: Short, z3: Long): Long = sketches.get(bin).map(_.estimateCount(z3)).getOrElse(0L)

  /**
    * Number of observations in the frequency map
    *
    * @return number of observations
    */
  def size: Long = sketches.values.map(_.size).sum

  /**
    * Split the stat into a separate stat per time bin of z data. Allows for separate handling of the reduced
    * data set.
    *
    * @return
    */
  def splitByTime: Seq[(Short, Z3Frequency)] = {
    sketches.toSeq.map { case (w, sketch) =>
      val freq = new Z3Frequency(sft, geom, dtg, period, precision, eps, confidence)
      freq.sketches.put(w, sketch)
      (w, freq)
    }
  }

  override def observe(sf: SimpleFeature): Unit = {
    val geom = sf.getAttribute(g).asInstanceOf[Geometry]
    val dtg  = sf.getAttribute(d).asInstanceOf[Date]
    if (geom != null && dtg != null) {
      try {
        val (bin, z3) = toKey(geom, dtg)
        sketches.getOrElseUpdate(bin, newSketch).add(z3, 1L)
      } catch {
        case e: Exception => logger.warn(s"Error observing geom '$geom' and date '$dtg': ${e.toString}")
      }
    }
  }

  // no-op
  override def unobserve(sf: SimpleFeature): Unit = {}

  override def +(other: Z3Frequency): Z3Frequency = {
    val plus = new Z3Frequency(sft, geom, dtg, period, precision, eps, confidence)
    plus += this
    plus += other
    plus
  }

  override def +=(other: Z3Frequency): Unit = {
    other.sketches.filter(_._2.size > 0).foreach { case (w, sketch) =>
      sketches.getOrElseUpdate(w, newSketch) += sketch
    }
  }

  override def clear(): Unit = sketches.values.foreach(_.clear())

  override def isEmpty: Boolean = sketches.values.forall(_.size == 0)

  override def toJsonObject: Any = {
    val (e, c) = sketches.values.headOption.map(s => (s.eps, s.confidence)).getOrElse((0d, 0d))
    ListMap("eps" -> e, "confidence" -> c, "size" -> size)
  }

  override def isEquivalent(other: Stat): Boolean = {
    other match {
      case s: Z3Frequency =>
        geom == s.geom && dtg == s.dtg && period == s.period && precision == s.precision && {
          val nonEmpty = sketches.filter(_._2.size > 0)
          val sNonEmpty = s.sketches.filter(_._2.size > 0)
          nonEmpty.keys == sNonEmpty.keys && nonEmpty.keys.forall { k =>
            sketches(k).isEquivalent(s.sketches(k))
          }
        }
      case _ => false
    }
  }
}
