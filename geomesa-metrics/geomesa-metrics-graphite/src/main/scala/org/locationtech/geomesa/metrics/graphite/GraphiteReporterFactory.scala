/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.metrics.graphite

import com.codahale.metrics.graphite.{Graphite, GraphiteReporter}
import com.codahale.metrics.{MetricRegistry, ScheduledReporter}
import com.typesafe.config.{Config, ConfigFactory}
import org.locationtech.geomesa.metrics.core.ReporterFactory
import pureconfig.{ConfigReader, ConfigSource}

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit
import javax.net.SocketFactory
import javax.net.ssl.SSLSocketFactory
import scala.util.control.NonFatal

class GraphiteReporterFactory extends ReporterFactory {

  import GraphiteReporterFactory.{Defaults, GraphiteConfig}

  override def apply(
      conf: Config,
      registry: MetricRegistry,
      rates: TimeUnit,
      durations: TimeUnit): Option[ScheduledReporter] = {
    if (!conf.hasPath("type") || !conf.getString("type").equalsIgnoreCase("graphite")) { None } else {
      val graphite = ConfigSource.fromConfig(conf.withFallback(Defaults)).loadOrThrow[GraphiteConfig]
      val url +: Nil :+ port = try { graphite.url.split(":").toList } catch {
        case NonFatal(_) => throw new IllegalArgumentException(s"Invalid url: ${graphite.url}")
      }
      val socketFactory = if (graphite.ssl) { SSLSocketFactory.getDefault } else { SocketFactory.getDefault }

      val reporter =
        GraphiteReporter.forRegistry(registry)
          .prefixedWith(graphite.prefix.orNull)
          .convertRatesTo(rates)
          .convertDurationsTo(durations)
          .build(new Graphite(new InetSocketAddress(url, port.toInt), socketFactory))

      Some(reporter)
    }
  }
}

object GraphiteReporterFactory {

  import pureconfig.generic.semiauto._

  private val Defaults = ConfigFactory.parseResourcesAnySyntax("graphite-defaults").resolve()

  case class GraphiteConfig(url: String, prefix: Option[String], ssl: Boolean)

  implicit val GraphiteConfigReader: ConfigReader[GraphiteConfig] = deriveReader[GraphiteConfig]
}
