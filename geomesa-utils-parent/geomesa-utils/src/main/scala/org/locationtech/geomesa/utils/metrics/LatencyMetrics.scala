/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.utils.metrics

import io.micrometer.core.instrument._
import org.geotools.api.feature.simple.SimpleFeature
import org.locationtech.geomesa.metrics.micrometer.utils.GaugeUtils

import java.util.Date
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{Duration, FiniteDuration}

/**
 * Latency metrics tracker
 *
 * @param dtgIndex index of the date attribute to track in the feature type
 * @param prefix prefix for metric names
 * @param tags metrics tags
 * @param maxExpectedLatency max expected latency for histogramming
 */
class LatencyMetrics(
    dtgIndex: Int,
    prefix: String,
    tags: java.lang.Iterable[Tag] = java.util.List.of(),
    maxExpectedLatency: FiniteDuration = Duration(1, TimeUnit.DAYS)) {

  private val date = GaugeUtils.timeGauge(s"$prefix.dtg.latest", tags)

  private val latency =
      DistributionSummary.builder(s"$prefix.dtg.latency")
        .tags(tags)
        .publishPercentileHistogram()
        .baseUnit("milliseconds")
        .minimumExpectedValue(1d)
        .maximumExpectedValue(maxExpectedLatency.toMillis.toDouble)
        .register(Metrics.globalRegistry)

  /**
   * Record latency for a feature
   *
   * @param feature feature
   */
  def apply(feature: SimpleFeature): Unit = {
    val dtg = feature.getAttribute(dtgIndex).asInstanceOf[Date]
    if (dtg != null) {
      date.set(dtg.getTime)
      latency.record(System.currentTimeMillis() - dtg.getTime)
    }
  }
}
