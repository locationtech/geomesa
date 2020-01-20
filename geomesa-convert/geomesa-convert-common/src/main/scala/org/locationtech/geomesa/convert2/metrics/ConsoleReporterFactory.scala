/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2.metrics

import java.util.Locale
import java.util.concurrent.TimeUnit

import com.codahale.metrics.Slf4jReporter.LoggingLevel
import com.codahale.metrics.{ConsoleReporter, MetricRegistry, ScheduledReporter, Slf4jReporter}
import com.typesafe.config.Config
import org.slf4j.LoggerFactory

class ConsoleReporterFactory extends ReporterFactory {

  override def apply(
      conf: Config,
      registry: MetricRegistry,
      rates: TimeUnit,
      durations: TimeUnit): Option[ScheduledReporter] = {
    if (!conf.hasPath("type") || !conf.getString("type").equalsIgnoreCase("console")) { None } else {
      val reporter =
        ConsoleReporter.forRegistry(registry)
          .convertRatesTo(rates)
          .convertDurationsTo(durations)
          .build()
      Some(reporter)
    }
  }
}


