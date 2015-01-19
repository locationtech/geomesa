/*
 *
 *  Copyright 2014 Commonwealth Computer Research, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the License);
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an AS IS BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.locationtech.geomesa.utils.stats

import java.util.concurrent.atomic.AtomicLong

import com.typesafe.scalalogging.slf4j.Logging

trait MethodProfiling {

  import java.lang.System.{currentTimeMillis => ctm}

  def profile[R](code: => R)(implicit timing: Timing): R = {
    val (startTime, r) = (ctm, code)
    timing.occurrence(ctm - startTime)
    r
  }

  def profile[R](code: => R, identifier: String)(implicit timings: Timings): R = {
    val (startTime, r) = (ctm, code)
    timings.occurrence(identifier, ctm - startTime)
    r
  }

  def profile[R](identifier: String)(code: => R)(implicit timings: Timings): R = profile(code, identifier)
}

/**
 * Class to hold timing results
 */
class Timing extends Serializable {

  private var total = 0L
  private var count = 0L

  /**
   * Updates this instance with a new timing
   *
   * @param time
   * @return
   */
  def occurrence(time: Long): Unit = {
    total += time
    count += 1
    this
  }

  /**
   * Gets the total time
   *
   * @return
   */
  def time: Long = total

  /**
   * Gets the number of event occurrences
   *
   * @return
   */
  def occurrences: Long = count

  /**
   * Computes the average for this instance
   *
   * @return
   */
  def average(): Double = total / count.toDouble
}

trait Timings extends Serializable {

  /**
   * Updates the given identifier with a new timing
   *
   * @param identifier
   * @param time
   */
  def occurrence(identifier: String, time: Long): Unit

  /**
   * Gets the total time for the given identifier
   *
   * @param identifier
   * @return
   */
  def time(identifier: String): Long

  /**
   * Gets the total occurrences for the given identifier
   *
   * @param identifier
   * @return
   */
  def occurrences(identifier: String): Long

  /**
   * Creates a printed string with the computed averages
   *
   * @return
   */
  def averageOccurrences(): String

  /**
   * Creates a printed string with the computed averages
   *
   * @return
   */
  def averageTimes(): String
}

/**
 * Class to hold timing results. Thread-safe.
 */
class TimingsImpl extends Timings {

  private val map = scala.collection.mutable.Map.empty[String, Timing]

  override def occurrence(identifier: String, time: Long): Unit = {
    val timing = map.synchronized(map.getOrElseUpdate(identifier, new Timing))
    timing.synchronized(timing.occurrence(time))
  }

  override def time(identifier: String): Long =
    map.synchronized(map.getOrElseUpdate(identifier, new Timing)).time

  override def occurrences(identifier: String): Long =
    map.synchronized(map.getOrElseUpdate(identifier, new Timing)).occurrences

  override def averageOccurrences(): String = if (map.isEmpty) {
    "No occurrences"
  } else {
    val entries = map.synchronized(map.toList).sortBy(_._1)
    val total = entries.map(_._2.occurrences).sum
    val percentOccurrences = entries.map { case (id, timing) =>
      s"$id: ${(timing.occurrences * 100 / total.toDouble).formatted("%.1f%%")}"
    }
    percentOccurrences.mkString(s"Total occurrences: $total. Percent of occurrences - ", ", ", "")
  }

  override def averageTimes(): String = if (map.isEmpty) {
    "No occurrences"
  } else {
    val entries = map.synchronized(map.toList).sortBy(_._1)
    val total = entries.map(_._2.time).sum
    val percentTimes = entries.map { case (id, timing) =>
      timing.synchronized(s"$id: ${(timing.time * 100 / total.toDouble).formatted("%.1f%%")}" +
          s" ${timing.average.formatted("%.4f")} ms avg")
    }
    percentTimes.mkString(s"Total time: $total ms. Percent of time - ", ", ", "")
  }
}

/**
 * Useful for sharing timings between instances of a certain class
 *
 * @param moduloToLog
 * @param logTimings
 * @param logOccurrences
 */
class AutoLoggingTimings(moduloToLog: Int = 1000, logTimings: Boolean = true, logOccurrences: Boolean = false)
    extends TimingsImpl with Logging {

  require(logTimings || logOccurrences, "Logging must be enabled for timings and/or occurrences")

  val count = new AtomicLong()

  val log = if (logTimings && logOccurrences) {
    () => {
      logger.debug(averageTimes())
      logger.debug(averageOccurrences())
    }
  } else if (logTimings) {
    () => logger.debug(averageTimes())
  } else {
    () => logger.debug(averageOccurrences())
  }

  override def occurrence(identifier: String, time: Long) = {
    super.occurrence(identifier, time)
    if (count.incrementAndGet() % moduloToLog == 0) log()
  }
}

class NoOpTimings extends Timings {

  override def occurrence(identifier: String, time: Long) = {}

  override def occurrences(identifier: String) = 0L

  override def time(identifier: String) = 0L

  override def averageTimes() = ""

  override def averageOccurrences() = ""
}