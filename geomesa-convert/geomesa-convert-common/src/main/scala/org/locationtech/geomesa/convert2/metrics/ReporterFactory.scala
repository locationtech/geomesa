/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2.metrics

import com.codahale.metrics.{MetricRegistry, ScheduledReporter}
import com.typesafe.config.Config

/**
  * Factory for SPI loading reporters at runtime
  */
@deprecated("replaced with org.locationtech.geomesa.metrics.core.ReporterFactory")
trait ReporterFactory extends org.locationtech.geomesa.metrics.core.ReporterFactory

@deprecated("replaced with org.locationtech.geomesa.metrics.core.ReporterFactory")
object ReporterFactory {

  /**
    * Load a reporter from the available factories
    *
    * @param config config
    * @param registry registry
    * @return
    */
  def apply(config: Config, registry: MetricRegistry): ScheduledReporter =
    org.locationtech.geomesa.metrics.core.ReporterFactory.apply(config, registry)
}
