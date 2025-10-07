/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.convert2.validators

import com.typesafe.scalalogging.LazyLogging
import io.micrometer.core.instrument.Tags
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.convert2.metrics.ConverterMetrics
import org.locationtech.geomesa.curve.BinnedTime
import org.locationtech.geomesa.utils.geotools.GeoToolsDateFormat
import org.locationtech.geomesa.utils.text.WKTUtils
import org.locationtech.jts.geom.Geometry

import java.time.Instant
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
      new Z3Validator(Attribute(sft, geom), Attribute(sft, dtg), minDate, maxDate, tags)
    } else if (enabled.contains("z2") || enabled.contains("xz2") || (enabled.isEmpty && geom != -1)) {
      new Z2Validator(Attribute(sft, geom), tags)
    } else {
      NoValidator
    }
  }
}

object IndexValidatorFactory extends LazyLogging {

  val Name = "index"

  // load a mutable envelope once - we don't modify it
  private val WholeWorldEnvelope = org.locationtech.geomesa.utils.geotools.wholeWorldEnvelope

  private def geomBoundsError(geom: Geometry): String =
    s"geometry exceeds world bounds ([-180,180][-90,90]): ${WKTUtils.write(geom)}"

  /**
   * Z2 validator
   *
   * @param geom geom
   * @param tags metrics tags
   */
  private class Z2Validator(geom: Attribute, tags: Tags) extends SimpleFeatureValidator {

    private val success = successCounter("z2", geom.name, tags)
    private val nullFailure = failureCounter("z2", geom.name, "null", tags)
    private val boundsFailure = failureCounter("z2", geom.name, "invalid.bounds", tags)

    override def validate(sf: SimpleFeature): String = {
      val g = sf.getAttribute(geom.i).asInstanceOf[Geometry]
      if (g == null) {
        nullFailure.increment()
        "geometry is null"
      } else if (!WholeWorldEnvelope.contains(g.getEnvelopeInternal)) {
        boundsFailure.increment()
        geomBoundsError(g)
      } else {
        success.increment()
        null
      }
    }

    override def close(): Unit = {}
  }

  /**
   * Z3 validator
   *
   * @param geom geom
   * @param dtg dtg
   * @param minDate min z3 date
   * @param maxDate max z3 date
   * @param tags metrics tags
   */
  private class Z3Validator(geom: Attribute, dtg: Attribute, minDate: Date, maxDate: Date, tags: Tags)
      extends SimpleFeatureValidator {

    private val success = successCounter("z3", s"${geom.name},${dtg.name}", tags)
    private val geomNullFailure = failureCounter("z3", geom.name, "null", tags)
    private val geomBoundsFailure = failureCounter("z3", geom.name, "invalid.bounds", tags)
    private val dtgNullFailure = failureCounter("z3", dtg.name, "null", tags)
    private val dtgBoundsFailure = failureCounter("z3", dtg.name, "invalid.bounds", tags)

    private val dateBefore = s"date is before minimum indexable date (${GeoToolsDateFormat.format(Instant.ofEpochMilli(minDate.getTime))}):"
    private val dateAfter = s"date is after maximum indexable date (${GeoToolsDateFormat.format(Instant.ofEpochMilli(maxDate.getTime))}):"

    override def validate(sf: SimpleFeature): String = {
      val d = sf.getAttribute(dtg.i).asInstanceOf[Date]
      val g = sf.getAttribute(geom.i).asInstanceOf[Geometry]
      var error: String = null
      if (g == null) {
        geomNullFailure.increment()
        error =  s"$error, geometry is null"
      } else if (!WholeWorldEnvelope.contains(g.getEnvelopeInternal)) {
        geomBoundsFailure.increment()
        error =  s"$error, ${geomBoundsError(g)}"
      }
      if (d == null) {
        dtgNullFailure.increment()
        error = s"$error, date is null"
      } else if (d.before(minDate)) {
        dtgBoundsFailure.increment()
        error = s"$error, $dateBefore ${GeoToolsDateFormat.format(Instant.ofEpochMilli(d.getTime))}"
      } else if (d.after(maxDate)) {
        dtgBoundsFailure.increment()
        error = s"$error, $dateAfter ${GeoToolsDateFormat.format(Instant.ofEpochMilli(d.getTime))}"
      }
      if (error == null) {
        success.increment()
        null
      } else {
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
