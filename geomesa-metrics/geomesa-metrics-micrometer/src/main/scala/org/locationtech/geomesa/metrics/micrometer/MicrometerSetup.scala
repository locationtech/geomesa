/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.metrics.micrometer

import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import io.micrometer.cloudwatch2.CloudWatchMeterRegistry
import io.micrometer.core.instrument.binder.jvm.{ClassLoaderMetrics, JvmGcMetrics, JvmMemoryMetrics, JvmThreadMetrics}
import io.micrometer.core.instrument.binder.system.ProcessorMetrics
import io.micrometer.core.instrument.{Clock, MeterRegistry, Metrics, Tag}
import io.micrometer.prometheusmetrics.{PrometheusMeterRegistry, PrometheusRenameFilter}
import io.prometheus.metrics.exporter.httpserver.HTTPServer
import io.prometheus.metrics.exporter.pushgateway.{Format, PushGateway, Scheme}
import org.locationtech.geomesa.utils.io.CloseWithLogging
import pureconfig.{ConfigReader, ConfigSource}
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient

import java.io.Closeable
import java.util.Locale
import java.util.concurrent.atomic.AtomicReference

object MicrometerSetup {

  import pureconfig.generic.semiauto._

  import scala.collection.JavaConverters._

  private val registries = scala.collection.mutable.Map.empty[String, String]
  private val bindings = scala.collection.mutable.Set.empty[String]

  val ConfigPath = "geomesa.metrics"

  /**
   * Add registries to the global registry list, based on the default configuration paths
   *
   * @param conf conf
   */
  def configure(conf: Config = ConfigFactory.load()): Unit = synchronized {
    if (conf.hasPath(ConfigPath)) {
      // noinspection ScalaUnusedSymbol
      implicit val bindingsReader: ConfigReader[MetricsBindings] = deriveReader[MetricsBindings]
      implicit val metricsReader: ConfigReader[MetricsConfig] = deriveReader[MetricsConfig]
      implicit val registryReader: ConfigReader[RegistryConfig] = deriveReader[RegistryConfig]
      val metricsConfig = ConfigSource.fromConfig(conf.getConfig(ConfigPath)).loadOrThrow[MetricsConfig]
      metricsConfig.registries.foreach { registryConfig =>
        val config = ConfigSource.fromConfig(registryConfig).loadOrThrow[RegistryConfig]
        if (config.enabled) {
          val configString = registryConfig.root().render(ConfigRenderOptions.concise())
          if (registries.contains(config.`type`)) {
            val existing = registries(config.`type`)
            if (existing != configString) {
              throw new IllegalArgumentException(
                s"Registry type ${config.`type`} already registered with a different configuration:" +
                  s"\n  existing: $existing\n    update: $configString")
            }
          } else {
            val registry = createRegistry(conf)
            sys.addShutdownHook(registry.close())
            Metrics.addRegistry(registry)
            registries.put(config.`type`, configString)
          }
        }
      }

      if (metricsConfig.bindings.classloader && bindings.add("classloader")) {
        new ClassLoaderMetrics().bindTo(Metrics.globalRegistry)
      }
      if (metricsConfig.bindings.memory && bindings.add("memory")) {
        new JvmMemoryMetrics().bindTo(Metrics.globalRegistry)
      }
      if (metricsConfig.bindings.gc && bindings.add("gc")) {
        new JvmGcMetrics().bindTo(Metrics.globalRegistry)
      }
      if (metricsConfig.bindings.processor && bindings.add("processor")) {
        new ProcessorMetrics().bindTo(Metrics.globalRegistry)
      }
      if (metricsConfig.bindings.threads && bindings.add("threads")) {
        new JvmThreadMetrics().bindTo(Metrics.globalRegistry)
      }
    }
  }

  /**
   * Create a new registry
   *
   * @param conf configuration for the registry
   * @return
   */
  def createRegistry(conf: Config): MeterRegistry = {
    implicit val reader: ConfigReader[RegistryConfig] = deriveReader[RegistryConfig]
    val config = ConfigSource.fromConfig(conf).loadOrThrow[RegistryConfig]
    config.`type`.toLowerCase(Locale.US) match {
      case "prometheus" => createPrometheusRegistry(conf)
      case "cloudwatch" => createCloudwatchRegistry(conf)
      case t => throw new IllegalArgumentException(s"No registry type defined for '$t' - valid values are: prometheus, cloudwatch")
    }
  }

  private def createPrometheusRegistry(conf: Config): PrometheusMeterRegistry = {
    // noinspection ScalaUnusedSymbol
    implicit val gatewayReader: ConfigReader[PushGatewayConfig] = deriveReader[PushGatewayConfig]
    implicit val prometheusReader: ConfigReader[PrometheusConfig] = deriveReader[PrometheusConfig]
    val config = ConfigSource.fromConfig(conf).loadOrThrow[PrometheusConfig]
    val dependentClose = new AtomicReference[Closeable]()
    val registry = new PrometheusMeterRegistry(k => config.properties.getOrElse(k, null)) {
      override def close(): Unit = {
        CloseWithLogging(Option(dependentClose.get()))
        super.close()
      }
    }
    registry.throwExceptionOnRegistrationFailure()
    if (config.rename) {
      registry.config().meterFilter(new PrometheusRenameFilter())
    }
    if (config.commonTags.nonEmpty) {
      val tags = config.commonTags.map { case (k, v) => Tag.of(k, v) }
      registry.config.commonTags(tags.asJava)
    }
    config.pushGateway match {
      case None =>
        val server =
          HTTPServer.builder()
            .port(config.port)
            .registry(registry.getPrometheusRegistry)
            .buildAndStart()
        dependentClose.set(server)

      case Some(pg) =>
        val builder = PushGateway.builder().registry(registry.getPrometheusRegistry).address(pg.host)
        pg.job.foreach(builder.job)
        pg.format.foreach(v => builder.format(Format.valueOf(v.toUpperCase(Locale.US))))
        pg.scheme.foreach(v => builder.scheme(Scheme.fromString(v.toLowerCase(Locale.US))))
        val pushGateway = builder.build()
        dependentClose.set(() => pushGateway.pushAdd())
    }
    registry
  }

  private def createCloudwatchRegistry(conf: Config): CloudWatchMeterRegistry = {
    implicit val reader: ConfigReader[CloudwatchConfig] = deriveReader[CloudwatchConfig]
    val config = ConfigSource.fromConfig(conf).loadOrThrow[CloudwatchConfig]
    new CloudWatchMeterRegistry(k => config.properties.getOrElse(k, null), Clock.SYSTEM, CloudWatchAsyncClient.create())
  }

  private case class MetricsConfig(
      registries: Seq[Config],
      bindings: MetricsBindings
    )

  private case class MetricsBindings(
      classloader: Boolean = false,
      memory: Boolean = false,
      gc: Boolean = false,
      processor: Boolean = false,
      threads: Boolean = false,
    )

  private case class RegistryConfig(
      `type`: String,
      enabled: Boolean = true,
    )

  private case class PrometheusConfig(
      rename: Boolean = false,
      commonTags: Map[String, String] = Map.empty,
      port: Int = 9090,
      // additional config can also be done via sys props - see https://prometheus.github.io/client_java/config/config/
      properties: Map[String, String] = Map.empty,
      pushGateway: Option[PushGatewayConfig],
    )

  private case class PushGatewayConfig(
      host: String,
      scheme: Option[String],
      job: Option[String],
      format: Option[String],
    )

  private case class CloudwatchConfig(
      namespace: String = "geomesa",
      properties: Map[String, String] = Map.empty
    )
}
