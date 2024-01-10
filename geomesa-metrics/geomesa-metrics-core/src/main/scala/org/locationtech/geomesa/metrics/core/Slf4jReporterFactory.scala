/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.metrics.core

import com.codahale.metrics.Slf4jReporter.LoggingLevel
import com.codahale.metrics.{MetricRegistry, ScheduledReporter, Slf4jReporter}
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.locationtech.geomesa.metrics.core.ReporterFactory.defaults
import org.slf4j.LoggerFactory
import pureconfig.{ConfigReader, ConfigSource}

import java.util.Locale
import java.util.concurrent.TimeUnit

class Slf4jReporterFactory extends ReporterFactory {

  import Slf4jReporterFactory.{Slf4jConfig, Slf4jDefaults}

  override def apply(
      conf: Config,
      registry: MetricRegistry,
      rates: TimeUnit,
      durations: TimeUnit): Option[ScheduledReporter] = {

    if (!conf.hasPath("type") || !conf.getString("type").equalsIgnoreCase("slf4j")) { None } else {
      val slf4j = ConfigSource.fromConfig(conf.withFallback(Slf4jDefaults)).loadOrThrow[Slf4jConfig]
      val logger = LoggerFactory.getLogger(slf4j.logger)
      val level = LoggingLevel.valueOf(slf4j.level.toUpperCase(Locale.US))
      val reporter =
        Slf4jReporter.forRegistry(registry)
          .outputTo(logger)
          .convertRatesTo(rates)
          .convertDurationsTo(durations)
          .withLoggingLevel(level)
          .build()
      Some(reporter)
    }
  }
}

object Slf4jReporterFactory {

  import pureconfig.generic.semiauto._

  val Slf4jDefaults: Config = ConfigFactory.empty.withValue("level", ConfigValueFactory.fromAnyRef("DEBUG"))

  implicit val Slf4jConfigReader: ConfigReader[Slf4jConfig] = deriveReader[Slf4jConfig]

  case class Slf4jConfig(logger: String, level: String)
}
