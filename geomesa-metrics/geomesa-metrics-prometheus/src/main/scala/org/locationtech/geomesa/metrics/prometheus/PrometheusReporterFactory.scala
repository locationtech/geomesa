/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.metrics.prometheus

import com.codahale.metrics._
import com.typesafe.config.{Config, ConfigFactory}
import io.prometheus.client.Collector.MetricFamilySamples
import io.prometheus.client.CollectorRegistry
import io.prometheus.client.dropwizard.DropwizardExports
import io.prometheus.client.dropwizard.samplebuilder.{DefaultSampleBuilder, SampleBuilder}
import io.prometheus.client.exporter.{HTTPServer, PushGateway}
import org.locationtech.geomesa.metrics.core.ReporterFactory
import org.locationtech.geomesa.utils.io.CloseWithLogging
import pureconfig.{ConfigReader, ConfigSource}

import java.net.URL
import java.util.Locale
import java.util.concurrent.TimeUnit

class PrometheusReporterFactory extends ReporterFactory {

  import PrometheusReporterFactory._

  override def apply(
      conf: Config,
      registry: MetricRegistry,
      rates: TimeUnit,
      durations: TimeUnit): Option[ScheduledReporter] = {
    val typ = if (conf.hasPath("type")) { Option(conf.getString("type")).map(_.toLowerCase(Locale.US)) } else { None }
    typ match {
      case Some("prometheus") =>
        val config = ConfigSource.fromConfig(conf.withFallback(Defaults)).loadOrThrow[PrometheusConfig]
        Some(new PrometheusReporter(registry, config.suffix, config.port))

      case Some("prometheus-pushgateway") =>
        val config = ConfigSource.fromConfig(conf.withFallback(Defaults)).loadOrThrow[PrometheusPushgatewayConfig]
        Some(new PushgatewayReporter(registry, config.suffix, config.gateway, config.jobName))

      case _ =>
        None
    }
  }
}

object PrometheusReporterFactory {

  import pureconfig.generic.semiauto._

  private val Defaults = ConfigFactory.parseResourcesAnySyntax("prometheus-defaults").resolve()

  case class PrometheusConfig(port: Int, suffix: Option[String])
  case class PrometheusPushgatewayConfig(gateway: URL, jobName: String, suffix: Option[String])

  implicit val PrometheusConfigReader: ConfigReader[PrometheusConfig] = deriveReader[PrometheusConfig]
  implicit val PrometheusPushgatewayConfigReader: ConfigReader[PrometheusPushgatewayConfig] =
    deriveReader[PrometheusPushgatewayConfig]

  /**
   * Prometheus reporter
   *
   * @param registry registry
   * @param suffix metrics suffix
   * @param port http server port, or zero to use an ephemeral open port
   */
  class PrometheusReporter(registry: MetricRegistry, suffix: Option[String], port: Int)
      extends BasePrometheusReporter(registry, CollectorRegistry.defaultRegistry, suffix) {

    private val server = new HTTPServer.Builder().withPort(port).build()

    def getPort: Int = server.getPort

    override def close(): Unit = {
      CloseWithLogging(server)
      super.close()
    }
  }

  /**
   * Pushgateway reporter
   *
   * Note: we use a new collector registry, as generally with pushgateway you don't want to expose things
   * from the default registry like memory, etc
   *
   * @param registry registry
   * @param suffix metrics suffix
   * @param gateway pushgateway url
   * @param jobName pushgateway job name
   */
  class PushgatewayReporter(registry: MetricRegistry, suffix: Option[String], gateway: URL, jobName: String)
      extends BasePrometheusReporter(registry, new CollectorRegistry(), suffix) {
    override def close(): Unit = {
      try { new PushGateway(gateway).pushAdd(collectorRegistry, jobName) } finally {
        super.close()
      }
    }
  }

  /**
   * Placeholder reporter that doesn't report any metrics, but handles the lifecycle of the prometheus
   * pusher or server
   *
   * @param registry registry
   * @param collectorRegistry prometheus registry
   * @param suffix metrics suffix
   */
  class BasePrometheusReporter(
      registry: MetricRegistry,
      protected val collectorRegistry: CollectorRegistry,
      suffix: Option[String]
    ) extends ScheduledReporter(registry, "prometheus", MetricFilter.ALL, TimeUnit.MILLISECONDS, TimeUnit.MILLISECONDS) {

    private val sampler = suffix match {
      case None => new DefaultSampleBuilder()
      case Some(s) => new SuffixSampleBuilder(s)
    }
    new DropwizardExports(registry, sampler).register(collectorRegistry)

    // since prometheus scrapes metrics, we don't have to report them here

    override def start(initialDelay: Long, period: Long, unit: TimeUnit): Unit = {}

    override def report(
        gauges: java.util.SortedMap[String, Gauge[_]],
        counters: java.util.SortedMap[String, Counter],
        histograms: java.util.SortedMap[String, Histogram],
        meters: java.util.SortedMap[String, Meter],
        timers: java.util.SortedMap[String, Timer]): Unit = {}
  }

  /**
   * Adds a suffix to each metric
   *
   * @param suffix suffix
   */
  class SuffixSampleBuilder(suffix: String) extends SampleBuilder {

    private val builder = new DefaultSampleBuilder()

    override def createSample(
        dropwizardName: String,
        nameSuffix: String,
        additionalLabelNames: java.util.List[String],
        additionalLabelValues: java.util.List[String],
        value: Double): MetricFamilySamples.Sample = {
      builder.createSample(dropwizardName, suffix, additionalLabelNames, additionalLabelValues, value)
    }
  }
}
