/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.convert2

import io.micrometer.core.instrument.{Counter, Metrics, Tags}
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.convert2.metrics.ConverterMetrics

package object validators {

  /**
   * Create a counter for successful validations
   *
   * @param validator validator name
   * @param attribute attribute name
   * @param tags common tags
   * @return
   */
  def successCounter(validator: String, attribute: String, tags: Tags): Counter =
    Metrics.counter(ConverterMetrics.name("validate"), tags.and(this.tags(validator, attribute, "pass")))

  /**
   * Create a counter for failed validations
   *
   * @param validator validator name
   * @param attribute attribute name
   * @param result result tag
   * @param tags common tags
   * @return
   */
  def failureCounter(validator: String, attribute: String, result: String, tags: Tags): Counter =
    Metrics.counter(ConverterMetrics.name("validate"), tags.and(this.tags(validator, attribute, result)))

  /**
   * Standardized tags
   */
  private def tags(validator: String, attribute: String, result: String): Tags =
    Tags.of("validator", validator, "attribute", attribute, "result", result)

  /**
   * Holder for an attribute name + index
   *
   * @param name name
   * @param i index
   */
  case class Attribute(name: String, i: Int)

  object Attribute {
    def apply(sft: SimpleFeatureType, i: Int): Attribute = Attribute(sft.getDescriptor(i).getLocalName, i)
  }

  /**
   * Validates an attribute is not null
   *
   * @param attribute attribute to validate
   * @param name attribute name
   * @param error error message
   * @param tags metric tags
   */
  class NullValidator(name: String, attribute: Attribute, error: String, tags: Tags) extends SimpleFeatureValidator {

    private val success = successCounter(name, attribute.name, tags)
    private val failure = failureCounter(name, attribute.name, "null", tags)

    override def validate(sf: SimpleFeature): String = {
      if (sf.getAttribute(attribute.i) != null) {
        success.increment()
        null
      } else {
        failure.increment()
        error
      }
    }

    override def close(): Unit = {}
  }

  case object NoValidator extends SimpleFeatureValidator {
    override def validate(sf: SimpleFeature): String = null
    override def close(): Unit = {}
  }
}
