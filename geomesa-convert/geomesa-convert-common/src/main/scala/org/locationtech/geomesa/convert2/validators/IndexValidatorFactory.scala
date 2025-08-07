/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.convert2.validators

import com.typesafe.scalalogging.LazyLogging
import io.micrometer.core.instrument.{Counter, Tags}
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.convert2.metrics.ConverterMetrics
import org.locationtech.geomesa.curve.BinnedTime
import org.locationtech.jts.geom.Geometry

import java.util.{Date, Locale}

/**
  * Validator based on the indices used by the feature type. Currently only the x/z2 and x/z3 indices have
  * input requirements. In addition, features that validate against the x/z3 index will also validate against
  * the x/z2 index
  */
class IndexValidatorFactory extends SimpleFeatureValidatorFactory {

  import IndexValidatorFactory._
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  override val name: String = IndexValidatorFactory.Name

  override def apply(sft: SimpleFeatureType, metrics: ConverterMetrics, config: Option[String]): SimpleFeatureValidator =
    apply(sft, config, Tags.empty())

  override def apply(sft: SimpleFeatureType, config: Option[String], tags: Tags): SimpleFeatureValidator = {
    val geom = sft.getGeomIndex
    val dtg = sft.getDtgIndex.getOrElse(-1)
    val enabled = sft.getIndices.collect { case id if id.mode.write => id.name.toLowerCase(Locale.US) }
    if (enabled.contains("z3") || enabled.contains("xz3") || (enabled.isEmpty && geom != -1 && dtg != -1)) {
      val minDate = Date.from(BinnedTime.ZMinDate.toInstant)
      val maxDate = Date.from(BinnedTime.maxDate(sft.getZ3Interval).toInstant)
      new Z3Validator(geom, dtg, minDate, maxDate, ErrorCounters("geom", tags), ErrorCounters("dtg", tags))
    } else if (enabled.contains("z2") || enabled.contains("xz2") || (enabled.isEmpty && geom != -1)) {
      new Z2Validator(geom, ErrorCounters("geom", tags))
    } else {
      NoValidator
    }
  }
}

object IndexValidatorFactory extends LazyLogging {

  val Name = "index"

  // load a mutable envelope once - we don't modify it
  private val WholeWorldEnvelope = org.locationtech.geomesa.utils.geotools.wholeWorldEnvelope

  private case class ErrorCounters(missing: Counter, bounds: Counter)

  private object ErrorCounters {
    def apply(field: String, tags: Tags): ErrorCounters =
      ErrorCounters(counter(s"$field.null", tags), counter(s"$field.bounds.invalid", tags))
  }

  /**
   * Z2 validator
   *
   * @param geom geom index
   * @param geomErrors geometry error counters
   */
  private class Z2Validator(geom: Int, geomErrors: ErrorCounters) extends SimpleFeatureValidator {
    override def validate(sf: SimpleFeature): String = {
      val g = sf.getAttribute(geom).asInstanceOf[Geometry]
      if (g == null) {
        geomErrors.missing.increment()
        Errors.GeomNull
      } else if (!WholeWorldEnvelope.contains(g.getEnvelopeInternal)) {
        geomErrors.bounds.increment()
        Errors.geomBounds(g)
      } else {
        null
      }
    }

    override def close(): Unit = {}
  }

  /**
   * Z3 validator
   *
   * @param geom geom index
   * @param dtg dtg index
   * @param minDate min z3 date
   * @param maxDate max z3 date
   * @param geomErrors geometry error counters
   * @param dtgErrors date error counters
   */
  private class Z3Validator(geom: Int, dtg: Int, minDate: Date, maxDate: Date, geomErrors: ErrorCounters, dtgErrors: ErrorCounters)
      extends SimpleFeatureValidator {

    private val dateBefore = Errors.dateBoundsLow(minDate)
    private val dateAfter = Errors.dateBoundsHigh(maxDate)

    override def validate(sf: SimpleFeature): String = {
      val d = sf.getAttribute(dtg).asInstanceOf[Date]
      val g = sf.getAttribute(geom).asInstanceOf[Geometry]
      var error: String = null
      if (g == null) {
        geomErrors.missing.increment()
        error =  s"$error, ${Errors.GeomNull}"
      } else if (!WholeWorldEnvelope.contains(g.getEnvelopeInternal)) {
        geomErrors.bounds.increment()
        error =  s"$error, ${Errors.geomBounds(g)}"
      }
      if (d == null) {
        dtgErrors.missing.increment()
        error = s"$error, ${Errors.DateNull}"
      } else if (d.before(minDate)) {
        dtgErrors.bounds.increment()
        error = s"$error, ${dateBefore(d)}"
      } else if (d.after(maxDate)) {
        dtgErrors.bounds.increment()
        error = s"$error, ${dateAfter(d)}"
      }
      if (error == null) { null } else {
        error.substring(6) // trim off leading 'null, '
      }
    }

    override def close(): Unit = {}
  }

  class ZIndexValidatorFactory extends IndexValidatorFactory {
    override val name = "z-index"
    override def apply(
        sft: SimpleFeatureType,
        metrics: ConverterMetrics,
        config: Option[String]): SimpleFeatureValidator = apply(sft, config, Tags.empty())
    override def apply(sft: SimpleFeatureType, config: Option[String], tags: Tags): SimpleFeatureValidator = {
      logger.warn("'z-index' validator is deprecated, using 'index' instead")
      super.apply(sft, config, tags)
    }
  }
}
