/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.metrics.micrometer
package prometheus

import com.typesafe.config.{ConfigFactory, ConfigValueFactory}

import java.io.Closeable

/**
 * Class for configuring prometheus metrics
 */
object PrometheusSetup extends RegistrySetup("prometheus") {

  /**
   * Register for a registry with the given settings
   *
   * @param application application tag used for metrics
   * @param port port used to serve metrics
   * @param rename rename metrics for prometheus
   * @return
   */
  def register(application: String = "geomesa", port: Int = 9090, rename: Boolean = true): Closeable = {
    val config =
      ConfigFactory.empty()
        .withValue("common-tags", ConfigValueFactory.fromMap(java.util.Map.of("application", application)))
        .withValue("port", ConfigValueFactory.fromAnyRef(port))
        .withValue("rename", ConfigValueFactory.fromAnyRef(rename))
        .withFallback(registry)
    register(config)
  }
}
