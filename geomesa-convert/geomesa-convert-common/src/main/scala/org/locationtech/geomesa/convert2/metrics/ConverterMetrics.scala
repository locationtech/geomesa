/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.convert2.metrics

import com.codahale.metrics._
import com.typesafe.config.Config
import io.micrometer.core.instrument.Tags
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.metrics.core.GeoMesaMetrics
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty

/**
  * Provides namespaced access to reporting metrics
  *
  * @param registry metric registry
  * @param prefix namespace prefix
  * @param typeName simple feature type name being processed
  * @param reporters metric reporters
  */
@deprecated("Use micrometer global registry")
class ConverterMetrics(
    registry: MetricRegistry,
    prefix: Option[String],
    typeName: String,
    reporters: Seq[ScheduledReporter]
  ) extends GeoMesaMetrics(registry, s"${prefix.map(_ + ".").getOrElse("")}geomesa.convert", reporters) {

  /**
    * Creates a prefixed counter
    *
    * @param id short identifier for the metric being counted
    * @return
    */
  def counter(id: String): Counter = super.counter(typeName, id)

  /**
    * Gets an updatable gauge
    *
    * @param id short identifier for hte metric being gauged
    * @tparam T gauge type
    * @return
    */
  def gauge[T](id: String): ConverterMetrics.SimpleGauge[T] =
    super.gauge(typeName, id, new ConverterMetrics.SimpleGauge()).asInstanceOf[ConverterMetrics.SimpleGauge[T]]

  /**
    * Creates a prefixed histogram
    *
    * @param id short identifier for the metric being histogram-ed
    * @return
    */
  def histogram(id: String): Histogram = super.histogram(typeName, id)

  /**
    * Creates a prefixed meter
    *
    * @param id short identifier for the metric being metered
    * @return
    */
  def meter(id: String): Meter = super.meter(typeName, id)

  /**
    * Creates a prefixed timer
    *
    * @param id short identifier for the metric being timed
    * @return
    */
  def timer(id: String): Timer = super.timer(typeName, id)

  /**
    * Register a metric
    *
    * @param id short identifier for the metric
    * @param metric metric to register
    * @tparam T metric type
    * @return
    */
  def register[T <: Metric](id: String, metric: T): T = super.register(typeName, id, metric)

  override def close(): Unit = {
    // execute a final report before closing, for situations where the converter runs too quickly to report anything
    try { reporters.foreach(_.report()) } finally {
      super.close()
    }
  }
}

object ConverterMetrics {

  @deprecated("replaced with micrometer/MetricsNamePrefix")
  val MetricsPrefix: SystemProperty = SystemProperty("geomesa.convert.validators.prefix")

  val MetricsNamePrefix: SystemProperty = SystemProperty("geomesa.convert.metrics.prefix", "geomesa.converter")

  /**
   * Gets a standard name for a converter-based metric, i.e. prefixing it with `geomesa.converter`
   *
   * @param name short name
   * @return
   */
  def name(name: String): String = {
    val prefix = MetricsNamePrefix.get
    if (prefix.isEmpty) { name } else { s"$prefix.$name" }
  }

  /**
   * Gets the converter name as a tag
   *
   * @param name converter name
   * @return
   */
  def converterNameTag(name: String): Tags = Tags.of("converter.name", name)

  /**
    * Creates an empty registry with no namespace or reporters
    *
    * @return
    */
  @deprecated("Use micrometer global registry")
  def empty: ConverterMetrics = new ConverterMetrics(new MetricRegistry(), None, "", Seq.empty)

  /**
    * Create a registry for the provided feature type
    *
    * @param sft simple feature type
    * @param reporters configs for metric reporters
    * @return
    */
  @deprecated("Use micrometer global registry")
  def apply(sft: SimpleFeatureType, reporters: Seq[Config]): ConverterMetrics = {
    val registry = new MetricRegistry()
    val reps = reporters.map(org.locationtech.geomesa.metrics.core.ReporterFactory.apply(_, registry)).toList
    new ConverterMetrics(registry, MetricsPrefix.option, sft.getTypeName, reps)
  }

  /**
    * Simple gauge that can be updated
    *
    * @tparam T value
    */
  @deprecated("Use micrometer global registry")
  class SimpleGauge[T] extends Gauge[T] {

    @volatile
    private var value: T = _

    override def getValue: T = value

    def set(value: T): Unit = this.value = value
  }
}
