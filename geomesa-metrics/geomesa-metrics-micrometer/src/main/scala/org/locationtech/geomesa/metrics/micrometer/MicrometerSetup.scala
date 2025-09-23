/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.metrics.micrometer

import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import com.typesafe.scalalogging.StrictLogging
import io.micrometer.core.instrument.{MeterRegistry, Metrics}
import org.locationtech.geomesa.metrics.micrometer.cloudwatch.CloudwatchFactory
import org.locationtech.geomesa.metrics.micrometer.prometheus.PrometheusFactory
import pureconfig.{ConfigReader, ConfigSource}

import java.util.concurrent.ConcurrentHashMap
import java.util.{Collections, Locale}
import scala.util.Try
import scala.util.control.NonFatal

@deprecated("Use PrometheusFactory or CloudwatchFactory")
object MicrometerSetup extends StrictLogging {

  import pureconfig.generic.semiauto._

  // registries and instrumentations that we've already bound, to prevent duplicates
  private val registries = Collections.newSetFromMap(new ConcurrentHashMap[String, java.lang.Boolean]())

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
  def configure(conf: Config): Unit = {
    if (conf.hasPath(ConfigPath)) {
      implicit val r0: ConfigReader[RegistryConfig] = deriveReader[RegistryConfig]
      val metricsConfig = MetricsConfig(conf.getConfig(ConfigPath))
      metricsConfig.registries.foreach { case (_, registryConfig) =>
        val config = ConfigSource.fromConfig(registryConfig).loadOrThrow[RegistryConfig]
        if (config.enabled) {
          val configString = registryConfig.root().render(ConfigRenderOptions.concise())
          if (registries.add(configString)) {
            val registry = createRegistry(registryConfig)
            sys.addShutdownHook(registry.close())
            Metrics.addRegistry(registry)
          }
        }
      }
      Instrumentations.bind(metricsConfig.instrumentations)
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
      case RegistryFactory.Prometheus => PrometheusFactory(conf)
      case RegistryFactory.Cloudwatch => CloudwatchFactory(conf)
      case t =>
        throw new IllegalArgumentException(
          s"No registry type defined for '$t' - valid values are: ${RegistryFactory.Prometheus}, ${RegistryFactory.Cloudwatch}")
    }
  }

  case class MetricsConfig(
      registries: Map[String, Config],
      instrumentations: Config,
    )

  object MetricsConfig {
    def apply(conf: Config): MetricsConfig = {
      implicit val r0: ConfigReader[MetricsConfig] = deriveReader[MetricsConfig]
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
      instrumentations: Config,
    )

  private case class RegistryConfig(
      `type`: String,
      enabled: Boolean = true,
    )
}
