/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2.validators

import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.convert2.metrics.ConverterMetrics

class NoneValidatorFactory extends SimpleFeatureValidatorFactory {
  override val name: String = "none"
  override def apply(
      sft: SimpleFeatureType,
      metrics: ConverterMetrics,
      config: Option[String]): SimpleFeatureValidator = NoValidator
}
