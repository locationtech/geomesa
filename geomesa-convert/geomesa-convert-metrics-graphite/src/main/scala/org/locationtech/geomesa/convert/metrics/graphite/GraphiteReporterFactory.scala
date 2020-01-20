/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.metrics.graphite

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit

import com.codahale.metrics.graphite.{Graphite, GraphiteReporter}
import com.codahale.metrics.{MetricRegistry, ScheduledReporter}
import com.typesafe.config.Config
import org.locationtech.geomesa.convert2.metrics.ReporterFactory
import pureconfig.ConfigReader

import scala.util.control.NonFatal

class GraphiteReporterFactory extends ReporterFactory {

  import GraphiteReporterFactory.GraphiteConfig

  override def apply(
      conf: Config,
      registry: MetricRegistry,
      rates: TimeUnit,
      durations: TimeUnit): Option[ScheduledReporter] = {
    if (!conf.hasPath("type") || !conf.getString("type").equalsIgnoreCase("graphite")) { None } else {
      val graphite = pureconfig.loadConfigOrThrow[GraphiteConfig](conf)
      val url +: Nil :+ port = try { graphite.url.split(":").toList } catch {
        case NonFatal(_) => throw new IllegalArgumentException(s"Invalid url: ${graphite.url}")
      }

      val reporter =
        GraphiteReporter.forRegistry(registry)
          .prefixedWith(graphite.prefix.orNull)
          .convertRatesTo(rates)
          .convertDurationsTo(durations)
          .build(new Graphite(new InetSocketAddress(url, port.toInt)))

      Some(reporter)
    }
  }
}

object GraphiteReporterFactory {

  import pureconfig.generic.semiauto._

  case class GraphiteConfig(url: String, prefix: Option[String])

  implicit val GraphiteConfigReader: ConfigReader[GraphiteConfig] = deriveReader[GraphiteConfig]
}
