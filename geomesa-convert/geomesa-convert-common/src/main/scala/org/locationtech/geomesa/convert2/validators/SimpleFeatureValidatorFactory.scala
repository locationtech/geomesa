/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2.validators

import org.locationtech.geomesa.convert2.metrics.ConverterMetrics
import org.opengis.feature.simple.SimpleFeatureType

trait SimpleFeatureValidatorFactory {

  /**
    * Well-known name of this validator, for specifying the validator to use
    *
    * @return
    */
  def name: String

  /**
    * Create a validator for the given feature typ
    *
    * @param sft simple feature type
    * @param metrics metrics registry for reporting validation
    * @param config optional configuration string
    */
  def apply(sft: SimpleFeatureType, metrics: ConverterMetrics, config: Option[String]): SimpleFeatureValidator
}
