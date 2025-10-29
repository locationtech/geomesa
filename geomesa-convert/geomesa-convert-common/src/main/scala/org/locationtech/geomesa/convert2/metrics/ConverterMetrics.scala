/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.convert2.metrics

import io.micrometer.core.instrument.Tags
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty

object ConverterMetrics {

  val MetricsNamePrefix: SystemProperty = SystemProperty("geomesa.convert.metrics.prefix", "geomesa.converter")

  /**
   * Gets a standard name for a converter-based metric, i.e. prefixing it with `geomesa.converter`
   *
   * @param name short name
   * @return
   */
  def name(name: String): String = {
    val prefix = MetricsNamePrefix.get
    if (prefix.isEmpty) { name } else { s"$prefix.$name" }
  }

  /**
   * Gets the converter name as a tag
   *
   * @param name converter name
   * @return
   */
  def converterNameTag(name: String): Tags = Tags.of("converter.name", name)
}
