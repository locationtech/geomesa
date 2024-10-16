/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.metrics.micrometer

import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import com.typesafe.scalalogging.StrictLogging
import io.micrometer.core.instrument.binder.jvm.{ClassLoaderMetrics, JvmGcMetrics, JvmMemoryMetrics, JvmThreadMetrics}
import io.micrometer.core.instrument.binder.system.ProcessorMetrics
import io.micrometer.core.instrument.{MeterRegistry, Metrics, Tag}
import org.locationtech.geomesa.metrics.micrometer.cloudwatch.CloudwatchFactory
import org.locationtech.geomesa.metrics.micrometer.prometheus.PrometheusFactory
import pureconfig.{ConfigReader, ConfigSource}

import java.util.Locale

object MicrometerSetup extends StrictLogging {

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
  // noinspection ScalaUnusedSymbol
  def configure(conf: Config = ConfigFactory.load()): Unit = synchronized {
    if (conf.hasPath(ConfigPath)) {
      implicit val r0: ConfigReader[Instrumentation] = deriveReader[Instrumentation]
      implicit val r1: ConfigReader[JvmInstrumentations] = deriveReader[JvmInstrumentations]
      implicit val r2: ConfigReader[MetricsConfig] = deriveReader[MetricsConfig]
      implicit val r3: ConfigReader[RegistryConfig] = deriveReader[RegistryConfig]
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
            val registry = createRegistry(registryConfig)
            sys.addShutdownHook(registry.close())
            Metrics.addRegistry(registry)
            registries.put(config.`type`, configString)
          }
        }
      }

      if (metricsConfig.instrumentations.classloader.enabled && bindings.add("classloader")) {
        logger.debug("Enabling JVM classloader metrics")
        val tags = metricsConfig.instrumentations.classloader.tags.map { case (k, v) => Tag.of(k, v) }
        new ClassLoaderMetrics(tags.asJava).bindTo(Metrics.globalRegistry)
      }
      if (metricsConfig.instrumentations.memory.enabled && bindings.add("memory")) {
        logger.debug("Enabling JVM memory metrics")
        val tags = metricsConfig.instrumentations.memory.tags.map { case (k, v) => Tag.of(k, v) }
        new JvmMemoryMetrics(tags.asJava).bindTo(Metrics.globalRegistry)
      }
      if (metricsConfig.instrumentations.gc.enabled && bindings.add("gc")) {
        logger.debug("Enabling JVM garbage collection metrics")
        val tags = metricsConfig.instrumentations.gc.tags.map { case (k, v) => Tag.of(k, v) }
        new JvmGcMetrics(tags.asJava).bindTo(Metrics.globalRegistry)
      }
      if (metricsConfig.instrumentations.processor.enabled && bindings.add("processor")) {
        logger.debug("Enabling JVM processor metrics")
        val tags = metricsConfig.instrumentations.processor.tags.map { case (k, v) => Tag.of(k, v) }
        new ProcessorMetrics(tags.asJava).bindTo(Metrics.globalRegistry)
      }
      if (metricsConfig.instrumentations.threads.enabled && bindings.add("threads")) {
        logger.debug("Enabling JVM thread metrics")
        val tags = metricsConfig.instrumentations.threads.tags.map { case (k, v) => Tag.of(k, v) }
        new JvmThreadMetrics(tags.asJava).bindTo(Metrics.globalRegistry)
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
      case "prometheus" => logger.debug("Creating prometheus registry"); PrometheusFactory(conf)
      case "cloudwatch" => logger.debug("Creating cloudwatch registry"); CloudwatchFactory(conf)
      case t => throw new IllegalArgumentException(s"No registry type defined for '$t' - valid values are: prometheus, cloudwatch")
    }
  }

  private case class MetricsConfig(
      registries: Seq[Config],
      instrumentations: JvmInstrumentations,
    )

  private case class Instrumentation(enabled: Boolean = false, tags: Map[String, String] = Map.empty)

  private case class JvmInstrumentations(
      classloader: Instrumentation = Instrumentation(),
      memory: Instrumentation = Instrumentation(),
      gc: Instrumentation = Instrumentation(),
      processor: Instrumentation = Instrumentation(),
      threads: Instrumentation = Instrumentation(),
    )

  private case class RegistryConfig(
      `type`: String,
      enabled: Boolean = true,
    )
}
