/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2.validators
import com.codahale.metrics.Counter
import org.locationtech.geomesa.convert2.metrics.ConverterMetrics
import org.locationtech.geomesa.convert2.validators.CqlValidatorFactory.CqlValidator
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

/**
  * Validator for arbitrary CQL filters
  */
class CqlValidatorFactory extends SimpleFeatureValidatorFactory {

  override def name: String = CqlValidatorFactory.Name

  override def apply(
      sft: SimpleFeatureType,
      metrics: ConverterMetrics,
      config: Option[String]): SimpleFeatureValidator = {
    val cql = config.getOrElse(throw new IllegalArgumentException("No filter specified for CQL Validator"))
    new CqlValidator(FastFilterFactory.toFilter(sft, cql), cql, metrics.counter("validators.cql"))
  }
}

object CqlValidatorFactory {

  val Name = "cql"

  class CqlValidator(filter: Filter, ecql: String, counter: Counter) extends SimpleFeatureValidator {

    private val error = s"failed filter: $ecql"

    override def validate(sf: SimpleFeature): String = {
      if (filter.evaluate(sf)) { null } else {
        counter.inc()
        error
      }
    }

    override def close(): Unit = {}
  }
}
