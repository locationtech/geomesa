/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert

import java.util.{Date, Locale, ServiceLoader}

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.jts.geom.Geometry
import org.locationtech.geomesa.curve.BinnedTime
import org.locationtech.geomesa.utils.geotools.wholeWorldEnvelope
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

trait SimpleFeatureValidator {

  /**
    * Initialize the validator with a simple feature type. Must be called before any calls to `validate`
    *
    * @param sft simple feature type
    */
  def init(sft: SimpleFeatureType): Unit

  /**
    * Validate a feature
    *
    * @param sf simple feature
    * @return validation error message, or null if valid
    */
  def validate(sf: SimpleFeature): String
}

object SimpleFeatureValidator extends LazyLogging {

  import scala.collection.JavaConverters._
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  val property = SystemProperty("geomesa.converter.validators", IndexValidator.name)

  def default: SimpleFeatureValidator = apply(property.get.split(","))

  def apply(names: Seq[String]): SimpleFeatureValidator = {
    val validators = names.map {
      case NoneValidator.name   => (NoneValidator, None)
      case IndexValidator.name  => (IndexValidator, None)
      case HasGeoValidator.name => (HasGeoValidator, None)
      case HasDtgValidator.name => (HasDtgValidator, None)

      case "z-index" =>
        logger.warn("'z-index' validator is deprecated, using 'index' instead")
        (IndexValidator, None)

      case other =>
        val factories = ServiceLoader.load(classOf[ValidatorFactory]).iterator().asScala
        val i = other.indexOf('(')
        val factory = if (i == -1 || !other.endsWith(")")) {
          factories.find(_.name == other).map(f => (f, None))
        } else {
          val name = other.substring(0, i)
          factories.find(_.name == name).map(f => (f, Some(other.substring(i + 1, other.length - 1))))
        }
        factory.getOrElse(throw new IllegalArgumentException(s"Unknown validator $other"))
    }

    if (validators.lengthCompare(2) < 0) {
      val (validator, config) = validators.headOption.getOrElse((NoneValidator, None))
      new FeatureValidator(validator, config)
    } else {
      new CompositeValidator(validators)
    }
  }

  def unapplySeq(validator: SimpleFeatureValidator): Option[Seq[String]] = {
    validator match {
      case f: FeatureValidator   => Some(f.names())
      case f: CompositeValidator => Some(f.names())
      case _ => None
    }
  }

  /**
    * Factory for validators
    */
  trait ValidatorFactory {
    def name: String
    def validator(sft: SimpleFeatureType, config: Option[String]): Validator
  }

  /**
    * Simplified validator interface for delegating
    */
  trait Validator {

    /**
      * Validates a feature
      *
      * @param sf feature
      * @return null if valid, otherwise error message
      */
    def validate(sf: SimpleFeature): String
  }

  /**
    * Simple feature validator implementation that delegates to a validator factory
    *
    * @param factory factory
    */
  class FeatureValidator(factory: ValidatorFactory, config: Option[String]) extends SimpleFeatureValidator {
    private var validator: Validator = _
    def names(): Seq[String] = Seq(factory.name + config.map(c => s"($c)").getOrElse(""))
    override def init(sft: SimpleFeatureType): Unit = validator = factory.validator(sft, config)
    override def validate(sf: SimpleFeature): String = validator.validate(sf)
  }

  /**
    * Evaluates multiple validators
    *
    * @param factories factories
    */
  class CompositeValidator(factories: Seq[(ValidatorFactory, Option[String])]) extends SimpleFeatureValidator {
    private var validators: Seq[Validator] = _
    def names(): Seq[String] =
      factories.map { case (factory, config) => factory.name + config.map(c => s"($c)").getOrElse("") }
    override def init(sft: SimpleFeatureType): Unit =
      validators = factories.map { case (factory, config) => factory.validator(sft, config) }
    override def validate(sf: SimpleFeature): String =
      validators.foldLeft(null: String) { (error, v) =>
        val valid = v.validate(sf)
        if (error == null) { valid } else if (valid == null ) { error } else { s"$error, $valid" }
      }
  }

  /**
    * Validator based on the indices used by the feature type. Currently only the x/z2 and x/z3 indices have
    * input requirements. In addition, features that validate against the x/z3 index will also validate against
    * the x/z2 index
    */
  object IndexValidator extends ValidatorFactory {
    override val name: String = "index"
    override def validator(sft: SimpleFeatureType, config: Option[String]): Validator = {
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

  object HasGeoValidator extends ValidatorFactory {
    override val name: String = "has-geo"
    override def validator(sft: SimpleFeatureType, config: Option[String]): Validator = {
      val i = sft.getGeomIndex
      if (i == -1) { NoValidator } else { new NullValidator(i, Errors.GeomNull) }
    }
  }

  object HasDtgValidator extends ValidatorFactory {
    override val name: String = "has-dtg"
    override def validator(sft: SimpleFeatureType, config: Option[String]): Validator = {
      val i = sft.getDtgIndex.getOrElse(-1)
      if (i == -1) { NoValidator } else { new NullValidator(i, Errors.DateNull) }
    }
  }

  object NoneValidator extends ValidatorFactory {
    override val name: String = "none"
    override def validator(sft: SimpleFeatureType, config: Option[String]): Validator = NoValidator
  }

  /**
    * Validates an attribute is not null
    *
    * @param i attribute index
    * @param error error message
    */
  private class NullValidator(i: Int, error: String) extends Validator {
    override def validate(sf: SimpleFeature): String = if (sf.getAttribute(i) == null) { error } else { null }
  }

  /**
    * Z2 validator
    *
    * @param geom geom index
    */
  private class Z2Validator(geom: Int) extends Validator {
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
  private class Z3Validator(geom: Int, dtg: Int, minDate: Date, maxDate: Date) extends Validator {
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

  private object NoValidator extends Validator {
    override def validate(sf: SimpleFeature): String = null
  }

  private object Errors {
    val GeomNull   = "geometry is null"
    val DateNull   = "date is null"
    val GeomBounds = s"geometry exceeds world bounds ($wholeWorldEnvelope)"
  }
}
