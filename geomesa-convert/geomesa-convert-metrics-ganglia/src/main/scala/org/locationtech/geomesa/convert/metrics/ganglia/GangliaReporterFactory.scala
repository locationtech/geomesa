/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.metrics.ganglia

import java.util.Locale
import java.util.concurrent.TimeUnit

import com.codahale.metrics.ganglia.GangliaReporter
import com.codahale.metrics.{MetricRegistry, ScheduledReporter}
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import info.ganglia.gmetric4j.gmetric.GMetric
import info.ganglia.gmetric4j.gmetric.GMetric.UDPAddressingMode
import org.locationtech.geomesa.convert2.metrics.ReporterFactory
import pureconfig.ConfigReader

class GangliaReporterFactory extends ReporterFactory {

  import GangliaReporterFactory.{GangliaConfig, GangliaDefaults}

  override def apply(
      conf: Config,
      registry: MetricRegistry,
      rates: TimeUnit,
      durations: TimeUnit): Option[ScheduledReporter] = {
    if (!conf.hasPath("type") || !conf.getString("type").equalsIgnoreCase("ganglia")) { None } else {
      val ganglia = pureconfig.loadConfigOrThrow[GangliaConfig](conf.withFallback(GangliaDefaults))
      val mode = ganglia.addressingMode.toLowerCase(Locale.US) match {
        case "unicast" => UDPAddressingMode.UNICAST
        case "multicast" => UDPAddressingMode.MULTICAST
        case m => throw new IllegalArgumentException(s"Invalid addressing mode: $m")
      }

      val reporter =
        GangliaReporter.forRegistry(registry)
          .convertRatesTo(rates)
          .convertDurationsTo(durations)
          .build(new GMetric(ganglia.group, ganglia.port, mode, ganglia.ttl, ganglia.ganglia311))

      Some(reporter)
    }
  }
}

object GangliaReporterFactory {

  import pureconfig.generic.semiauto._

  val GangliaDefaults: Config =
    ConfigFactory.empty
        .withValue("addressing-mode", ConfigValueFactory.fromAnyRef("unicast"))
        .withValue("ganglia311", ConfigValueFactory.fromAnyRef("true"))

  implicit val GangliaConfigReader: ConfigReader[GangliaConfig] = deriveReader[GangliaConfig]

  case class GangliaConfig(group: String, port: Int, addressingMode: String, ttl: Int, ganglia311: Boolean)
}
