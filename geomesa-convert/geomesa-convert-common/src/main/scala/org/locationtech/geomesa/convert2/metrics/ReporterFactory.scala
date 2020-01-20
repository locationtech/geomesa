/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2.metrics

import java.util.concurrent.TimeUnit

import com.codahale.metrics.{MetricRegistry, ScheduledReporter}
import com.typesafe.config.Config
import org.locationtech.geomesa.utils.classpath.ServiceLoader
import pureconfig.error.{CannotConvert, ConfigReaderFailures}
import pureconfig.{ConfigCursor, ConfigObjectCursor, ConfigReader}

import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

/**
  * Factory for SPI loading reporters at runtime
  */
trait ReporterFactory {
  def apply(conf: Config, registry: MetricRegistry, rates: TimeUnit, durations: TimeUnit): Option[ScheduledReporter]
}

object ReporterFactory {

  private lazy val factories = ServiceLoader.load[ReporterFactory]()

  private implicit val reader: ConfigReader[ReporterConfig] = ReporterReader

  /**
    * Load a reporter from the available factories
    *
    * @param config config
    * @param registry registry
    * @return
    */
  def apply(config: Config, registry: MetricRegistry): ScheduledReporter = {
    val ReporterConfig(rates, durations, interval) = pureconfig.loadConfigOrThrow[ReporterConfig](config)
    val reporter = factories.toStream.flatMap(_.apply(config, registry, rates, durations)).headOption.getOrElse {
      throw new IllegalArgumentException(s"Could not load reporter factory using provided config:\n" +
          config.root().render())
    }
    if (interval > 0) {
      reporter.start(interval, TimeUnit.MILLISECONDS)
    }
    reporter
  }

  case class ReporterConfig(rates: TimeUnit, durations: TimeUnit, interval: Long)

  object ReporterReader extends ConfigReader[ReporterConfig] {
    override def from(cur: ConfigCursor): Either[ConfigReaderFailures, ReporterConfig] = {
      for {
        obj      <- cur.asObjectCursor.right
        rate     <- timeUnit(obj, "rate-units", "units").right
        duration <- timeUnit(obj, "duration-units", "units").right
        interval <- durationMillis(obj.atKeyOrUndefined("interval")).right
      } yield {
        ReporterConfig(rate, duration, interval)
      }
    }

    private def timeUnit(cur: ConfigObjectCursor, key: String, fallback: String): Either[ConfigReaderFailures, TimeUnit] = {
      val primary = cur.atKey(key).right.flatMap(_.asString).right.flatMap { unit =>
        TimeUnit.values().find(_.toString.equalsIgnoreCase(unit)).toRight[ConfigReaderFailures](
          ConfigReaderFailures(cur.failureFor(CannotConvert(cur.value.toString, "TimeUnit", "Does not match a TimeUnit")))
        )
      }
      if (primary.isRight || fallback == null) { primary } else {
        // use the fallback if it's a right, otherwise use the primary as a left
        timeUnit(cur, fallback, null).left.flatMap(_ => primary)
      }
    }

    private def durationMillis(cur: ConfigCursor): Either[ConfigReaderFailures, Long] = {
      if (cur.isUndefined) { Right(-1L) } else {
        cur.asString.right.flatMap { d =>
          try { Right(Duration(d).toMillis) } catch {
            case NonFatal(e) => cur.failed(CannotConvert(cur.value.toString, "Duration", e.getMessage))
          }
        }
      }
    }
  }
}
