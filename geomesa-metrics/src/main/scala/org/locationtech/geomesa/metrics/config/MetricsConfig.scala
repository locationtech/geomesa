/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.metrics.config

import java.net.InetSocketAddress
import java.util.Locale
import java.util.concurrent.TimeUnit

import com.codahale.metrics.Slf4jReporter.LoggingLevel
import com.codahale.metrics.ganglia.GangliaReporter
import com.codahale.metrics.graphite.{Graphite, GraphiteReporter}
import com.codahale.metrics.{MetricRegistry, _}
import com.typesafe.config.{ConfigFactory, Config}
import com.typesafe.scalalogging.LazyLogging
import info.ganglia.gmetric4j.gmetric.GMetric
import info.ganglia.gmetric4j.gmetric.GMetric.UDPAddressingMode
import org.locationtech.geomesa.metrics.reporters.{DelimitedFileReporter, AccumuloReporter}
import org.slf4j.LoggerFactory

@deprecated("Will be removed without replacement")
object MetricsConfig extends LazyLogging {

  val ConfigPath = "geomesa.metrics.reporters"

  def reporters(config: Config,
                registry: MetricRegistry,
                path: Option[String] = Some(ConfigPath),
                start: Boolean = true): Seq[ScheduledReporter] = {
    val reporters = path match {
      case Some(p) => if (config.hasPath(p)) config.getConfig(p) else ConfigFactory.empty()
      case None    => config
    }

    import scala.collection.JavaConversions._
    reporters.root.keys.toSeq.flatMap { path =>
      try {
        Some(createReporter(reporters.getConfig(path), registry, start))
      } catch {
        case e: Exception =>
          logger.warn("Invalid reporter config", e)
          None
      }
    }
  }

  private def createReporter(config: Config, registry: MetricRegistry, start: Boolean): ScheduledReporter = {
    val rate = timeUnit(config.getString(if (config.hasPath("rate-units")) "rate-units" else "units"))
    val duration = timeUnit(config.getString(if (config.hasPath("duration-units")) "duration-units" else "units"))
    val interval = if (config.hasPath("interval")) config.getInt("interval") else -1
    val typ = config.getString("type").toLowerCase(Locale.US)

    val reporter = if (typ == "console") {
      ConsoleReporter.forRegistry(registry)
          .convertRatesTo(rate)
          .convertDurationsTo(duration)
          .build()
    } else if (typ == "slf4j") {
      val logger = LoggerFactory.getLogger(config.getString("logger"))
      val level = if (config.hasPath("level")) {
        LoggingLevel.valueOf(config.getString("level").toUpperCase)
      } else {
        LoggingLevel.DEBUG
      }
      Slf4jReporter.forRegistry(registry)
          .outputTo(logger)
          .convertRatesTo(rate)
          .convertDurationsTo(duration)
          .withLoggingLevel(level)
          .build()
    } else if (typ == "delimited-text") {
      val path = config.getString("output")
      val aggregate = if (config.hasPath("aggregate")) config.getBoolean("aggregate") else true
      val tabs = if (config.hasPath("tabs")) config.getBoolean("tabs") else true

      val builder = DelimitedFileReporter.forRegistry(registry)
          .convertRatesTo(rate)
          .convertDurationsTo(duration)
          .aggregate(aggregate)
      if (tabs) {
        builder.withTabs()
      } else {
        builder.withCommas()
      }
      builder.build(path)
    } else if (typ == "graphite") {
      val url +: Nil :+ port = config.getString("url").split(":").toList
      val graphite = new Graphite(new InetSocketAddress(url, port.toInt))
      val prefix = if (config.hasPath("prefix")) config.getString("prefix") else null

      GraphiteReporter.forRegistry(registry)
          .prefixedWith(prefix)
          .convertRatesTo(rate)
          .convertDurationsTo(duration)
          .build(graphite)
    } else if (typ == "ganglia") {
      val group = config.getString("group")
      val port = config.getInt("port")
      val mode = if (config.hasPath("addressing-mode") &&
          config.getString("addressing-mode").equalsIgnoreCase("unicast")) {
        UDPAddressingMode.UNICAST
      } else {
        UDPAddressingMode.MULTICAST
      }
      val ttl = config.getInt("ttl")
      val is311 = if (config.hasPath("ganglia311")) config.getBoolean("ganglia311") else true
      val ganglia = new GMetric(group, port.toInt, mode, ttl, is311)

      GangliaReporter.forRegistry(registry)
          .convertRatesTo(rate)
          .convertDurationsTo(duration)
          .build(ganglia)
    } else if (typ == "accumulo") {
      val instance = config.getString("instanceId")
      val zoos = config.getString("zookeepers")
      val user = config.getString("user")
      val password = config.getString("password")
      val table = config.getString("tableName")

      val builder = AccumuloReporter.forRegistry(registry)
          .convertRatesTo(rate)
          .convertDurationsTo(duration)
          .writeToTable(table)
      if (config.hasPath("visibilities")) {
        builder.withVisibilities(config.getString("visibilities"))
      }
      if (config.hasPath("mock") && config.getBoolean("mock")) {
        builder.mock(true)
      }
      builder.build(instance, zoos, user, password)
    } else {
      throw new RuntimeException(s"No reporter type '$typ' defined")
    }

    if (start && interval > 0) {
      reporter.start(interval, TimeUnit.SECONDS)
    }

    reporter
  }

  // convert string to timeunit enum using reflection
  private def timeUnit(unit: String): TimeUnit =
    classOf[TimeUnit].getField(unit.toUpperCase(Locale.US)).get(null).asInstanceOf[TimeUnit]
}
