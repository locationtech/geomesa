/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.convert2.validators

import io.micrometer.core.instrument.Tags
import org.geotools.api.feature.simple.SimpleFeatureType

trait SimpleFeatureValidatorFactory {

  /**
    * Well-known name of this validator, for specifying the validator to use
    *
    * @return
    */
  def name: String

  /**
   * Create a validator for the given feature type
   *
   * The default implementation will be removed in the next major release
   *
   * @param sft simple feature type
   * @param config optional configuration string
   * @param tags for metrics
   */
  def apply(sft: SimpleFeatureType, config: Option[String], tags: Tags): SimpleFeatureValidator
}
