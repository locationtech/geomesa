/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.metrics.micrometer

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging

import java.util.Locale

/**
 * Class for configuring metrics
 *
 * @param name name of registry, and associated resource file
 */
class RegistrySetup(name: String) extends StrictLogging {

  /**
   * Configure default metrics. Metrics can be customized with environment variables, or with an application.conf,
   * see `<name>.conf` for details
   */
  def configure(): Unit = {
    val conf = ConfigFactory.load(name.toLowerCase(Locale.US))
    if (conf.getBoolean("enabled")) {
      logger.info(s"Configuring $name metrics")
      MicrometerSetup.configure(conf.atPath(MicrometerSetup.ConfigPath).withFallback(ConfigFactory.load()))
    } else {
      logger.info(s"$name metrics disabled")
    }
  }
}
