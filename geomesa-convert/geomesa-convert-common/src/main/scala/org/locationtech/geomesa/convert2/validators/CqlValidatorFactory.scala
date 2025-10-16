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
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.convert2.metrics.ConverterMetrics
import org.locationtech.geomesa.convert2.validators.CqlValidatorFactory.CqlValidator
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.filter.factory.FastFilterFactory

/**
  * Validator for arbitrary CQL filters
  */
class CqlValidatorFactory extends SimpleFeatureValidatorFactory {

  override def name: String = CqlValidatorFactory.Name

  override def apply(sft: SimpleFeatureType, metrics: ConverterMetrics, config: Option[String]): SimpleFeatureValidator =
    apply(sft, config, Tags.empty())

  override def apply(sft: SimpleFeatureType, config: Option[String], tags: Tags): SimpleFeatureValidator = {
    val ecql = config.getOrElse(throw new IllegalArgumentException("No filter specified for CQL Validator"))
    val filter = FastFilterFactory.toFilter(sft, ecql)
    new CqlValidator(filter, ecql, tags)
  }
}

object CqlValidatorFactory {

  val Name = "cql"

  class CqlValidator(filter: Filter, ecql: String, tags: Tags) extends SimpleFeatureValidator {

    private val attributes = FilterHelper.propertyNames(filter).sorted.mkString(",")

    private val success = successCounter("cql", attributes, tags)
    private val failure = failureCounter("cql", attributes, "filtered", tags)
    private val error = s"failed filter: $ecql"

    override def validate(sf: SimpleFeature): String = {
      if (filter.evaluate(sf)) {
        success.increment()
        null
      } else {
        failure.increment()
        error
      }
    }

    override def close(): Unit = {}
  }
}
