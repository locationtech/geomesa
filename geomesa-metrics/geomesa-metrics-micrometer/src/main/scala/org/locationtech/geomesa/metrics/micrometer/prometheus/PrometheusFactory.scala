/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.metrics.micrometer
package prometheus
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import io.micrometer.core.instrument.{MeterRegistry, Tag}
import io.micrometer.prometheusmetrics.{PrometheusMeterRegistry, PrometheusRenameFilter}
import io.prometheus.metrics.exporter.httpserver.HTTPServer
import io.prometheus.metrics.exporter.pushgateway.{Format, PushGateway, Scheme}
import pureconfig.generic.semiauto.deriveReader
import pureconfig.{ConfigReader, ConfigSource}

import java.io.Closeable
import java.util.Locale
import java.util.concurrent.atomic.AtomicReference
import scala.util.control.NonFatal

object PrometheusFactory extends RegistryFactory with LazyLogging {

  import scala.collection.JavaConverters._

  override def apply(conf: Config): MeterRegistry = {
    // noinspection ScalaUnusedSymbol
    implicit val gatewayReader: ConfigReader[PushGatewayConfig] = deriveReader[PushGatewayConfig]
    implicit val prometheusReader: ConfigReader[PrometheusConfig] = deriveReader[PrometheusConfig]
    val config = ConfigSource.fromConfig(conf).loadOrThrow[PrometheusConfig]
    val dependentClose = new AtomicReference[Closeable]()
    val registry = new PrometheusMeterRegistry(k => config.properties.getOrElse(k, null)) {
      override def close(): Unit = {
        val child = dependentClose.get()
        if (child != null) {
          try { child.close() } catch {
            case NonFatal(e) => logger.error("Error on close:", e)
          }
        }
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
}
