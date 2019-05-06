/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2.validators

import java.util.{Date, Locale}

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.curve.BinnedTime
import org.locationtech.geomesa.utils.classpath.ServiceLoader
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.geotools.wholeWorldEnvelope
import org.locationtech.jts.geom.Geometry
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

trait SimpleFeatureValidator {

  /**
    * Validate a feature
    *
    * @param sf simple feature
    * @return validation error message, or null if valid
    */
  def validate(sf: SimpleFeature): String
}

object SimpleFeatureValidator extends LazyLogging {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

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
    * @return
    */
  def apply(sft: SimpleFeatureType, names: Seq[String]): SimpleFeatureValidator = {
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
      factory.apply(sft, options)
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
        override def apply(sft: SimpleFeatureType, config: Option[String]): SimpleFeatureValidator = {
          new SimpleFeatureValidator() {
            private val validator = factory.validator(sft, config)
            override def validate(sf: SimpleFeature): String = validator.validate(sf)
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
  }

  /**
    * Validator based on the indices used by the feature type. Currently only the x/z2 and x/z3 indices have
    * input requirements. In addition, features that validate against the x/z3 index will also validate against
    * the x/z2 index
    */
  class IndexValidatorFactory extends SimpleFeatureValidatorFactory {
    override val name: String = IndexValidatorFactory.Name
    override def apply(sft: SimpleFeatureType, config: Option[String]): SimpleFeatureValidator = {
      val geom = sft.getGeomIndex
      val dtg = sft.getDtgIndex.getOrElse(-1)
      val enabled = sft.getIndices.collect { case id if id.mode.write => id.name.toLowerCase(Locale.US) }
      if (enabled.contains("z3") || enabled.contains("xz3") || (enabled.isEmpty && geom != -1 && dtg != -1)) {
        val minDate = Date.from(BinnedTime.ZMinDate.toInstant)
        val maxDate = Date.from(BinnedTime.maxDate(sft.getZ3Interval).toInstant)
        new Z3Validator(geom, dtg, minDate, maxDate)
      } else if (enabled.contains("z2") || enabled.contains("xz2") || (enabled.isEmpty && geom != -1)) {
        new Z2Validator(geom)
      } else {
        NoValidator
      }
    }
  }

  object IndexValidatorFactory {
    val Name = "index"
  }

  class ZIndexValidatorFactory extends IndexValidatorFactory {
    override val name = "z-index"
    override def apply(sft: SimpleFeatureType, config: Option[String]): SimpleFeatureValidator = {
      logger.warn("'z-index' validator is deprecated, using 'index' instead")
      super.apply(sft, config)
    }
  }

  class HasGeoValidatorFactory extends SimpleFeatureValidatorFactory {
    override val name: String = HasGeoValidatorFactory.Name
    override def apply(sft: SimpleFeatureType, config: Option[String]): SimpleFeatureValidator = {
      val i = sft.getGeomIndex
      if (i == -1) { NoValidator } else { new NullValidator(i, Errors.GeomNull) }
    }
  }

  object HasGeoValidatorFactory {
    val Name = "has-geo"
  }

  class HasDtgValidatorFactory extends SimpleFeatureValidatorFactory {
    override val name: String = HasDtgValidatorFactory.Name
    override def apply(sft: SimpleFeatureType, config: Option[String]): SimpleFeatureValidator = {
      val i = sft.getDtgIndex.getOrElse(-1)
      if (i == -1) { NoValidator } else { new NullValidator(i, Errors.DateNull) }
    }
  }

  object HasDtgValidatorFactory {
    val Name = "has-dtg"
  }

  class NoneValidatorFactory extends SimpleFeatureValidatorFactory {
    override val name: String = "none"
    override def apply(sft: SimpleFeatureType, config: Option[String]): SimpleFeatureValidator = NoValidator
  }

  /**
    * Validates an attribute is not null
    *
    * @param i attribute index
    * @param error error message
    */
  private class NullValidator(i: Int, error: String) extends SimpleFeatureValidator {
    override def validate(sf: SimpleFeature): String = if (sf.getAttribute(i) == null) { error } else { null }
  }

  /**
    * Z2 validator
    *
    * @param geom geom index
    */
  private class Z2Validator(geom: Int) extends SimpleFeatureValidator {
    override def validate(sf: SimpleFeature): String = {
      val g = sf.getAttribute(geom).asInstanceOf[Geometry]
      if (g == null) {
        Errors.GeomNull
      } else if (!wholeWorldEnvelope.contains(g.getEnvelopeInternal)) {
        Errors.GeomBounds
      } else {
        null
      }
    }
  }

  /**
    * Z3 validator
    *
    * @param geom geom index
    * @param dtg dtg index
    * @param minDate min z3 date
    * @param maxDate max z3 date
    */
  private class Z3Validator(geom: Int, dtg: Int, minDate: Date, maxDate: Date) extends SimpleFeatureValidator {
    private val dateBefore = s"date is before minimum indexable date ($minDate)"
    private val dateAfter = s"date is after maximum indexable date ($maxDate)"

    override def validate(sf: SimpleFeature): String = {
      val d = sf.getAttribute(dtg).asInstanceOf[Date]
      val g = sf.getAttribute(geom).asInstanceOf[Geometry]
      if (g == null) {
        if (d == null) {
          s"${Errors.GeomNull}, ${Errors.DateNull}"
        } else if (d.before(minDate)) {
          s"${Errors.GeomNull}, $dateBefore"
        } else if (d.after(maxDate)) {
          s"${Errors.GeomNull}, $dateAfter"
        } else {
          Errors.GeomNull
        }
      } else if (!wholeWorldEnvelope.contains(g.getEnvelopeInternal)) {
        if (d == null) {
          s"${Errors.GeomBounds}, ${Errors.DateNull}"
        } else if (d.before(minDate)) {
          s"${Errors.GeomBounds}, $dateBefore"
        } else if (d.after(maxDate)) {
          s"${Errors.GeomBounds}, $dateAfter"
        } else {
          Errors.GeomBounds
        }
      } else if (d == null) {
        Errors.DateNull
      } else if (d.before(minDate)) {
        dateBefore
      } else if (d.after(maxDate)) {
        dateAfter
      } else {
        null
      }
    }
  }

  private object NoValidator extends SimpleFeatureValidator {
    override def validate(sf: SimpleFeature): String = null
  }

  private object Errors {
    val GeomNull   = "geometry is null"
    val DateNull   = "date is null"
    val GeomBounds = s"geometry exceeds world bounds ($wholeWorldEnvelope)"
  }
}
