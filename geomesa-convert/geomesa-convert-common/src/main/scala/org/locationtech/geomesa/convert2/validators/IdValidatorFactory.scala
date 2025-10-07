/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.convert2.validators

import io.micrometer.core.instrument.Tags
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.convert2.metrics.ConverterMetrics
import org.locationtech.geomesa.convert2.validators.IdValidatorFactory.IdValidator

/**
 * Validates a feature ID is not null
 */
class IdValidatorFactory extends SimpleFeatureValidatorFactory {

  override val name: String = IdValidatorFactory.Name

  override def apply(sft: SimpleFeatureType, metrics: ConverterMetrics, config: Option[String]): SimpleFeatureValidator =
    apply(sft, config, Tags.empty())

  override def apply(sft: SimpleFeatureType, config: Option[String], tags: Tags): SimpleFeatureValidator =
    new IdValidator(tags)
}

object IdValidatorFactory {

  val Name = "id"

  /**
   * Validates a feature ID is not null
   *
   * @param tags metric tags
   */
  class IdValidator(tags: Tags) extends SimpleFeatureValidator {

    private val success = successCounter("id", "id", tags)
    private val failure = failureCounter("id", "id", "null", tags)

    override def validate(sf: SimpleFeature): String = {
      if (sf.getID != null && sf.getID.nonEmpty) {
        success.increment()
        null
      } else {
        failure.increment()
        "feature ID is null"
      }
    }

    override def close(): Unit = {}
  }
}
