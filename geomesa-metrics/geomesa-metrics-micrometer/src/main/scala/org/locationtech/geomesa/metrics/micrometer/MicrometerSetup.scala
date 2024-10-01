/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.metrics.micrometer

import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import io.micrometer.core.instrument.binder.jvm.{ClassLoaderMetrics, JvmGcMetrics, JvmMemoryMetrics, JvmThreadMetrics}
import io.micrometer.core.instrument.binder.system.ProcessorMetrics
import io.micrometer.core.instrument.{MeterRegistry, Metrics}
import org.locationtech.geomesa.metrics.micrometer.cloudwatch.CloudwatchFactory
import org.locationtech.geomesa.metrics.micrometer.prometheus.PrometheusFactory
import pureconfig.{ConfigReader, ConfigSource}

import java.util.Locale

object MicrometerSetup {

  import pureconfig.generic.semiauto._

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
      case "prometheus" => PrometheusFactory(conf)
      case "cloudwatch" => CloudwatchFactory(conf)
      case t => throw new IllegalArgumentException(s"No registry type defined for '$t' - valid values are: prometheus, cloudwatch")
    }
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
}
