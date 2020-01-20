/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2.validators

import java.util.{Date, Locale}

import com.codahale.metrics.Counter
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.convert2.metrics.ConverterMetrics
import org.locationtech.geomesa.curve.BinnedTime
import org.locationtech.jts.geom.Geometry
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
  * Validator based on the indices used by the feature type. Currently only the x/z2 and x/z3 indices have
  * input requirements. In addition, features that validate against the x/z3 index will also validate against
  * the x/z2 index
  */
class IndexValidatorFactory extends SimpleFeatureValidatorFactory {

  import IndexValidatorFactory._
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  override val name: String = IndexValidatorFactory.Name
  override def apply(
      sft: SimpleFeatureType,
      metrics: ConverterMetrics,
      config: Option[String]): SimpleFeatureValidator = {
    val geom = sft.getGeomIndex
    val dtg = sft.getDtgIndex.getOrElse(-1)
    val enabled = sft.getIndices.collect { case id if id.mode.write => id.name.toLowerCase(Locale.US) }
    if (enabled.contains("z3") || enabled.contains("xz3") || (enabled.isEmpty && geom != -1 && dtg != -1)) {
      val minDate = Date.from(BinnedTime.ZMinDate.toInstant)
      val maxDate = Date.from(BinnedTime.maxDate(sft.getZ3Interval).toInstant)
      val counters = {
        val geom = ErrorCounters(metrics.counter(GeomNullCounter), metrics.counter(GeomBoundsCounter))
        val dtg = ErrorCounters(metrics.counter(DtgNullCounter), metrics.counter(DtgBoundsCounter))
        Z3Counters(geom, dtg, metrics.counter(Z3TotalCounter))
      }
      new Z3Validator(geom, dtg, minDate, maxDate, counters)
    } else if (enabled.contains("z2") || enabled.contains("xz2") || (enabled.isEmpty && geom != -1)) {
      val counters = {
        val geom = ErrorCounters(metrics.counter(GeomNullCounter), metrics.counter(GeomBoundsCounter))
        Z2Counters(geom, metrics.counter(Z2TotalCounter))
      }
      new Z2Validator(geom, counters)
    } else {
      NoValidator
    }
  }
}

object IndexValidatorFactory extends LazyLogging {

  val Name = "index"

  val GeomNullCounter = "validators.geom.null"
  val GeomBoundsCounter = "validators.geom.bounds"

  val DtgNullCounter = "validators.dtg.null"
  val DtgBoundsCounter = "validators.dtg.bounds"

  val Z2TotalCounter = "validators.z2.failed"
  val Z3TotalCounter = "validators.z3.failed"

  // load a mutable envelope once - we don't modify it
  private val WholeWorldEnvelope = org.locationtech.geomesa.utils.geotools.wholeWorldEnvelope

  private case class ErrorCounters(missing: Counter, bounds: Counter)

  private case class Z2Counters(geom: ErrorCounters, total: Counter)
  private case class Z3Counters(geom: ErrorCounters, dtg: ErrorCounters, total: Counter)

  /**
    * Z2 validator
    *
    * @param geom geom index
    */
  private class Z2Validator(geom: Int, counters: Z2Counters) extends SimpleFeatureValidator {
    override def validate(sf: SimpleFeature): String = {
      val g = sf.getAttribute(geom).asInstanceOf[Geometry]
      if (g == null) {
        counters.geom.missing.inc()
        counters.total.inc()
        Errors.GeomNull
      } else if (!WholeWorldEnvelope.contains(g.getEnvelopeInternal)) {
        counters.geom.bounds.inc()
        counters.total.inc()
        Errors.GeomBounds
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
    */
  private class Z3Validator(geom: Int, dtg: Int, minDate: Date, maxDate: Date, counters: Z3Counters)
      extends SimpleFeatureValidator {

    private val dateBefore = s"date is before minimum indexable date ($minDate)"
    private val dateAfter = s"date is after maximum indexable date ($maxDate)"

    override def validate(sf: SimpleFeature): String = {
      val d = sf.getAttribute(dtg).asInstanceOf[Date]
      val g = sf.getAttribute(geom).asInstanceOf[Geometry]
      var error: String = null
      if (g == null) {
        counters.geom.missing.inc()
        error =  s"$error, ${Errors.GeomNull}"
      } else if (!WholeWorldEnvelope.contains(g.getEnvelopeInternal)) {
        counters.geom.bounds.inc()
        error =  s"$error, ${Errors.GeomBounds}"
      }
      if (d == null) {
        counters.dtg.missing.inc()
        error = s"$error, ${Errors.DateNull}"
      } else if (d.before(minDate)) {
        counters.dtg.bounds.inc()
        error = s"$error, $dateBefore"
      } else if (d.after(maxDate)) {
        counters.dtg.bounds.inc()
        error = s"$error, $dateAfter"
      }
      if (error == null) { null } else {
        counters.total.inc()
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
        config: Option[String]): SimpleFeatureValidator = {
      logger.warn("'z-index' validator is deprecated, using 'index' instead")
      super.apply(sft, metrics, config)
    }
  }
}
