/***********************************************************************
 * Copyright (c) 2013-2025 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.metrics.micrometer

import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import com.typesafe.scalalogging.StrictLogging
import io.micrometer.core.instrument.binder.MeterBinder
import io.micrometer.core.instrument.binder.commonspool2.CommonsObjectPool2Metrics
import io.micrometer.core.instrument.binder.jvm.{ClassLoaderMetrics, JvmGcMetrics, JvmMemoryMetrics, JvmThreadMetrics}
import io.micrometer.core.instrument.binder.system.ProcessorMetrics
import io.micrometer.core.instrument.{MeterRegistry, Metrics, Tag}
import org.locationtech.geomesa.metrics.micrometer.cloudwatch.CloudwatchFactory
import org.locationtech.geomesa.metrics.micrometer.prometheus.PrometheusFactory
import pureconfig.{ConfigReader, ConfigSource}

import java.util.Locale
import scala.util.Try
import scala.util.control.NonFatal

object MicrometerSetup extends StrictLogging {

  import pureconfig.generic.semiauto._

  import scala.collection.JavaConverters._

  // note: access to these is done inside a 'synchronized' block
  private val registries = scala.collection.mutable.Map.empty[String, String]
  private val bindings = scala.collection.mutable.Set.empty[(String, Map[String, String])]

  val ConfigPath = "geomesa.metrics"

  /**
   * Add registries to the global registry list, based on the default configuration paths
   */
  def configure(): Unit = configure(ConfigFactory.load())

  /**
   * Add registries to the global registry list, based on the default configuration paths
   *
   * @param conf conf
   */
  def configure(conf: Config): Unit = synchronized {
    if (conf.hasPath(ConfigPath)) {
      implicit val r0: ConfigReader[RegistryConfig] = deriveReader[RegistryConfig]
      val MetricsConfig(registries, instrumentations) = MetricsConfig(conf.getConfig(ConfigPath))
      registries.foreach { case (_, registryConfig) =>
        val config = ConfigSource.fromConfig(registryConfig).loadOrThrow[RegistryConfig]
        if (config.enabled) {
          val configString = registryConfig.root().render(ConfigRenderOptions.concise())
          if (this.registries.contains(config.`type`)) {
            val existing = this.registries(config.`type`)
            if (existing != configString) {
              throw new IllegalArgumentException(
                s"Registry type ${config.`type`} already registered with a different configuration:" +
                  s"\n  existing: $existing\n    update: $configString")
            }
          } else {
            val registry = createRegistry(registryConfig)
            sys.addShutdownHook(registry.close())
            Metrics.addRegistry(registry)
            this.registries.put(config.`type`, configString)
          }
        }
      }

      addBinding(instrumentations.classloader, "JVM classloader", tags => new ClassLoaderMetrics(tags))
      addBinding(instrumentations.memory,      "JVM memory",      tags => new JvmMemoryMetrics(tags))
      addBinding(instrumentations.gc,          "JVM gc",          tags => new JvmGcMetrics(tags))
      addBinding(instrumentations.processor,   "JVM processor",   tags => new ProcessorMetrics(tags))
      addBinding(instrumentations.threads,     "JVM thread",      tags => new JvmThreadMetrics(tags))
      addBinding(instrumentations.commonsPool, "commons pool",    tags => new CommonsObjectPool2Metrics(tags))
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

  /**
   * Add a meter binding
   *
   * @param config config
   * @param key unique key to identify this binding
   * @param binder binder constructor
   */
  private def addBinding(config: Instrumentation, key: String, binder: java.lang.Iterable[Tag] => MeterBinder): Unit = {
    if (config.enabled && bindings.add(key -> config.tags)) {
      val tags = config.tags.map { case (k, v) => Tag.of(k, v) }
      logger.debug(s"Enabling $key metrics with tags: ${tags.toSeq.map(t => s"${t.getKey}=${t.getValue}").sorted.mkString(", ")}")
      binder(tags.asJava).bindTo(Metrics.globalRegistry)
    }
  }

  case class MetricsConfig(
      registries: Map[String, Config],
      instrumentations: Instrumentations,
    )

  object MetricsConfig {
    // noinspection ScalaUnusedSymbol
    def apply(conf: Config): MetricsConfig = {
      implicit val r0: ConfigReader[Instrumentation] = deriveReader[Instrumentation]
      implicit val r1: ConfigReader[Instrumentations] = deriveReader[Instrumentations]
      implicit val r2: ConfigReader[MetricsConfig] = deriveReader[MetricsConfig]
      val source = ConfigSource.fromConfig(conf)
      try { source.loadOrThrow[MetricsConfig] } catch {
        case NonFatal(e) =>
          // back compatible loading attempt
          implicit val r: ConfigReader[MetricsSeqConfig] = deriveReader[MetricsSeqConfig]
          val c = Try(source.loadOrThrow[MetricsSeqConfig]).getOrElse(throw e)
          val registries = Seq.tabulate(c.registries.length)(i => String.valueOf(i)).zip(c.registries).toMap
          MetricsConfig(registries, c.instrumentations)
      }
    }
  }

  // back compatible structure
  private case class MetricsSeqConfig(
      registries: Seq[Config],
      instrumentations: Instrumentations,
    )

  case class Instrumentation(enabled: Boolean = false, tags: Map[String, String] = Map.empty)

  case class Instrumentations(
      classloader: Instrumentation = Instrumentation(),
      memory: Instrumentation = Instrumentation(),
      gc: Instrumentation = Instrumentation(),
      processor: Instrumentation = Instrumentation(),
      threads: Instrumentation = Instrumentation(),
      commonsPool: Instrumentation = Instrumentation(),
    )

  private case class RegistryConfig(
      `type`: String,
      enabled: Boolean = true,
    )
}
