/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.metrics.micrometer.utils

import io.micrometer.core.instrument.{Metrics, Tag, Tags, TimeGauge}

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

object GaugeUtils {

  import scala.collection.JavaConverters._

  private val registrations = new ConcurrentHashMap[String, AtomicInteger]()

  /**
   * Register a map size gauge.
   *
   * Adds a unique registration tag to ensure the gauge is captured correctly.
   *
   * @param name metric name
   * @param tags tags
   * @param map map to measure
   * @tparam T map type
   * @return the original map
   */
  def mapSizeGauge[T <: java.util.Map[_, _]](name: String, tags: java.lang.Iterable[Tag], map: T): T =
    Metrics.gaugeMapSize(name, addUidTag(name, tags), map)

  /**
   * Register a new time gauge.
   *
   * Adds a unique registration tag to ensure the gauge is captured correctly.
   *
   * @param name metric name
   * @param tags tags
   * @param timeUnit time unit of the gauge
   * @return
   */
  def timeGauge(name: String, tags: java.lang.Iterable[Tag], timeUnit: TimeUnit = TimeUnit.MILLISECONDS): AtomicLong = {
    val time = new AtomicLong()
    TimeGauge.builder(name, () => Long.box(time.get), timeUnit).tags(addUidTag(name, tags)).register(Metrics.globalRegistry)
    time
  }

  private def addUidTag(name: String, tags: java.lang.Iterable[Tag]): Tags = {
    val cacheKey = name + tags.asScala.toList.sorted.mkString(" ", ",", "")
    val uid = registrations.computeIfAbsent(cacheKey, _ => new AtomicInteger(0)).getAndIncrement().toString
    Tags.of("uid", uid).and(tags)
  }
}
