/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
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

  val DefaultValidators: SystemProperty = SystemProperty("geomesa.converter.validators", IndexValidatorFactory.Name)

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
      val factory = factories.find(_.name.equalsIgnoreCase(name)).getOrElse {
        throw new IllegalArgumentException(s"No factory found for name '$name'. " +
            s"Available factories: ${factories.map(_.name).mkString(", ")}")
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
