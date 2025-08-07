/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.metrics.micrometer
package prometheus

import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.prometheus.metrics.exporter.pushgateway.{Format, Scheme}

import java.io.Closeable

/**
 * Class for configuring prometheus metrics
 */
object PrometheusSetup extends RegistrySetup("prometheus") {

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
        .withFallback(registry)
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
        .withFallback(registry)
    register(config)
  }
}
