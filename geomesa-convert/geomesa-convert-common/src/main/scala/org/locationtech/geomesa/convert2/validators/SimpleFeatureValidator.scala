/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2.validators

import java.io.Closeable

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.convert2.metrics.ConverterMetrics
import org.locationtech.geomesa.utils.classpath.ServiceLoader
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

trait SimpleFeatureValidator extends Closeable {

  /**
    * Validate a feature
    *
    * @param sf simple feature
    * @return validation error message, or null if valid
    */
  def validate(sf: SimpleFeature): String
}

object SimpleFeatureValidator extends LazyLogging {

  private lazy val factories = ServiceLoader.load[SimpleFeatureValidatorFactory]()

  // noinspection ScalaDeprecation
  private lazy val factoriesV1 =
    ServiceLoader.load[org.locationtech.geomesa.convert.SimpleFeatureValidator.ValidatorFactory]()

  val DefaultValidators = SystemProperty("geomesa.converter.validators", IndexValidatorFactory.Name)

  /**
    * Default validator names
    *
    * @return
    */
  def default: Seq[String] = DefaultValidators.get.split(",")

  /**
    * Create validators for the given feature type
    *
    * @param sft simple feature type
    * @param names validator names and options
    * @param metrics optional metrics registry for tracking validation results
    * @return
    */
  def apply(sft: SimpleFeatureType, names: Seq[String], metrics: ConverterMetrics): SimpleFeatureValidator = {
    val validators = names.map { full =>
      val i = full.indexOf('(')
      val (name, options) = if (i == -1) { (full, None) } else {
        require(full.last == ')', s"Invalid option parentheses: $full")
        (full.substring(0, i), Some(full.substring(i + 1, full.length - 1)))
      }
      val factory = factories.find(_.name.equalsIgnoreCase(name)).orElse(v1(name)).getOrElse {
        throw new IllegalArgumentException(s"No factory found for name '$name'. " +
            s"Available factories: ${(factories.map(_.name) ++ factoriesV1.map(_.name)).mkString(", ")}")
      }
      factory.apply(sft, metrics, options)
    }

    if (validators.lengthCompare(2) < 0) {
      validators.headOption.getOrElse(NoValidator)
    } else {
      new CompositeValidator(validators)
    }
  }

  /**
    * Wrapper for custom version 1 validators
    *
    * @param name validator name
    * @return
    */
  private def v1(name: String): Option[SimpleFeatureValidatorFactory] = {
    factoriesV1.find(_.name.equalsIgnoreCase(name)).map { factory =>
      logger.warn(s"Using deprecated validator API for factory '${factory.getClass.getName}'. " +
          s"Please migrate to org.locationtech.geomesa.convert2.validators.SimpleFeatureValidatorFactory")
      new SimpleFeatureValidatorFactory() {
        override def name: String = factory.name
        override def apply(
            sft: SimpleFeatureType,
            metrics: ConverterMetrics,
            config: Option[String]): SimpleFeatureValidator = {
          new SimpleFeatureValidator() {
            private val validator = factory.validator(sft, config)
            override def validate(sf: SimpleFeature): String = validator.validate(sf)
            override def close(): Unit = {}
          }
        }
      }
    }
  }

  /**
    * Evaluates multiple validators
    *
    * @param validators validators
    */
  class CompositeValidator(validators: Seq[SimpleFeatureValidator]) extends SimpleFeatureValidator {
    override def validate(sf: SimpleFeature): String = {
      var error: String = null
      validators.foreach { validator =>
        val e = validator.validate(sf)
        if (e != null) {
          error = if (error == null) { e } else { s"$error, $e" }
        }
      }
      error
    }
    override def close(): Unit = CloseWithLogging(validators)
  }
}
