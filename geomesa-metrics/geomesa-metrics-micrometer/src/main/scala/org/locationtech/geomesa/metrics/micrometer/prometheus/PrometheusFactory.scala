/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.metrics.micrometer
package prometheus

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import io.micrometer.core.instrument.{MeterRegistry, Tag}
import io.micrometer.prometheusmetrics.{PrometheusMeterRegistry, PrometheusRenameFilter}
import io.prometheus.metrics.exporter.httpserver.HTTPServer
import io.prometheus.metrics.exporter.pushgateway.{Format, PushGateway, Scheme}
import org.locationtech.geomesa.metrics.micrometer.RegistryFactory.BaseRegistryFactory
import pureconfig.generic.semiauto.deriveReader
import pureconfig.{ConfigReader, ConfigSource}

import java.io.Closeable
import java.util.Locale
import java.util.concurrent.atomic.AtomicReference
import scala.util.control.NonFatal

object PrometheusFactory extends BaseRegistryFactory(RegistryFactory.Prometheus) {

  import scala.collection.JavaConverters._

  override protected def createRegistry(conf: Config): MeterRegistry = {
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

  /**
   * Register for a registry with the given settings
   *
   * @param port port used to serve metrics
   * @param application application tag used for metrics
   * @param rename rename metrics for prometheus
   * @return
   */
  def register(port: Int = 9090, application: String = "geomesa", rename: Boolean = true): Closeable = {
    val config =
      ConfigFactory.empty()
        .withValue("common-tags", ConfigValueFactory.fromMap(java.util.Map.of("application", application)))
        .withValue("port", ConfigValueFactory.fromAnyRef(port))
        .withValue("rename", ConfigValueFactory.fromAnyRef(rename))
    register(config)
  }

  /**
   * Register for a registry with the given settings
   *
   * @param host push gateway host + port
   * @param job job name
   * @param scheme push scheme
   * @param format push format
   * @param application application tag used for metrics
   * @param rename rename metrics for prometheus
   * @return
   */
  def registerPushGateway(
      host: String,
      job: String,
      scheme: Scheme = Scheme.HTTP,
      format: Format = Format.PROMETHEUS_PROTOBUF,
      application: String = "geomesa",
      rename: Boolean = true): Closeable = {
    val pgConfig =
      ConfigFactory.empty()
        .withValue("host", ConfigValueFactory.fromAnyRef(host))
        .withValue("job", ConfigValueFactory.fromAnyRef(job))
        .withValue("scheme", ConfigValueFactory.fromAnyRef(scheme.toString))
        .withValue("format", ConfigValueFactory.fromAnyRef(format.toString))
    val config =
      ConfigFactory.empty()
        .withValue("common-tags", ConfigValueFactory.fromMap(java.util.Map.of("application", application)))
        .withValue("rename", ConfigValueFactory.fromAnyRef(rename))
        .withValue("push-gateway", pgConfig.root())
    register(config)
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
