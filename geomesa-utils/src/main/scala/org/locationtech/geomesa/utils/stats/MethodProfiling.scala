/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.stats

import java.util.concurrent.atomic.AtomicLong

import com.typesafe.scalalogging.LazyLogging

trait MethodProfiling extends LazyLogging {

  protected def profile[R](onComplete: Long => Unit)(code: => R): R = {
    val start = System.currentTimeMillis
    val result: R = code
    onComplete(System.currentTimeMillis - start)
    result
  }

  protected def profile[R](onComplete: (R, Long) => Unit)(code: => R): R = {
    val start = System.currentTimeMillis
    val result: R = code
    onComplete(result, System.currentTimeMillis - start)
    result
  }

  protected def profile[R](message: String)(code: => R): R =
    profile(time => logger.debug(s"$message in ${time}ms"))(code)
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
   * @param time time in millis
   * @return
   */
  def occurrence(time: Long): Unit = {
    total += time
    count += 1
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
   * @param identifier identifier
   * @param time time in millis
   */
  def occurrence(identifier: String, time: Long): Unit

  /**
   * Gets the total time for the given identifier
   *
   * @param identifier identifier
   * @return
   */
  def time(identifier: String): Long

  /**
   * Gets the total occurrences for the given identifier
   *
   * @param identifier identifier
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
 * Class to hold timing results. Not thread-safe.
 */
class TimingsImpl extends Timings {

  private val map = scala.collection.mutable.Map.empty[String, Timing]

  override def occurrence(identifier: String, time: Long): Unit =
    map.getOrElseUpdate(identifier, new Timing).occurrence(time)

  override def time(identifier: String): Long = map.getOrElseUpdate(identifier, new Timing).time

  override def occurrences(identifier: String): Long = map.getOrElseUpdate(identifier, new Timing).occurrences

  override def averageOccurrences(): String = if (map.isEmpty) {
    "No occurrences"
  } else {
    val entries = map.toList.sortBy(_._1)
    val total = entries.map(_._2.occurrences).sum
    val percentOccurrences = entries.map { case (id, timing) =>
      s"$id: ${(timing.occurrences * 100 / total.toDouble).formatted("%.1f%%")}"
    }
    percentOccurrences.mkString(s"Total occurrences: $total. Percent of occurrences - ", ", ", "")
  }

  override def averageTimes(): String = if (map.isEmpty) {
    "No occurrences"
  } else {
    val entries = map.toList.sortBy(_._1)
    val total = entries.map(_._2.time).sum
    val percentTimes = entries.map { case (id, timing) =>
      s"$id: ${(timing.time * 100 / total.toDouble).formatted("%.1f%%")}" +
          s" ${timing.occurrences} times at ${timing.average().formatted("%.4f")} ms avg"
    }
    percentTimes.mkString(s"Total time: $total ms. Percent of time - ", ", ", "")
  }
}

/**
 * Class to hold timing results. Thread-safe.
 */
class ThreadSafeTimingsImpl extends Timings {

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
          s" ${timing.occurrences} times at ${timing.average().formatted("%.4f")} ms avg")
    }
    percentTimes.mkString(s"Total time: $total ms. Percent of time - ", ", ", "")
  }
}

/**
 * Useful for sharing timings between instances of a certain class
 *
 * @param moduloToLog how many events to skip between logging
 */
class AutoLoggingTimings(moduloToLog: Int = 1000) extends ThreadSafeTimingsImpl with LazyLogging {

  val count = new AtomicLong()

  override def occurrence(identifier: String, time: Long): Unit = {
    super.occurrence(identifier, time)
    if (count.incrementAndGet() % moduloToLog == 0) {
      logger.debug(averageTimes())
    }
  }
}

object NoOpTimings extends Timings {

  override def occurrence(identifier: String, time: Long): Unit = {}

  override def occurrences(identifier: String) = 0L

  override def time(identifier: String) = 0L

  override def averageTimes() = ""

  override def averageOccurrences() = ""
}