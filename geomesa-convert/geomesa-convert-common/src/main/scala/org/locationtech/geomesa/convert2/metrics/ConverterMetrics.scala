/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2.metrics

import com.codahale.metrics.MetricRegistry.MetricSupplier
import com.codahale.metrics._
import com.typesafe.config.Config
import org.locationtech.geomesa.convert2.metrics.ConverterMetrics.SimpleGauge
import org.locationtech.geomesa.metrics.core.GeoMesaMetrics
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.opengis.feature.simple.SimpleFeatureType

/**
  * Provides namespaced access to reporting metrics
  *
  * @param registry metric registry
  * @param prefix namespace prefix
  * @param typeName simple feature type name being processed
  * @param reporters metric reporters
  */
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
  def gauge[T](id: String): SimpleGauge[T] =
    super.gauge(typeName, id, ConverterMetrics.GaugeSupplier).asInstanceOf[SimpleGauge[T]]

  /**
    * Creates a prefixed histogram
    *
    * @param id short identifier for the metric being histogramed
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

  val MetricsPrefix: SystemProperty = SystemProperty("geomesa.convert.validators.prefix")

  private val GaugeSupplier = new MetricSupplier[Gauge[_]] {
    override def newMetric(): Gauge[_] = new SimpleGauge()
  }

  /**
    * Creates an empty registry with no namespace or reporters
    *
    * @return
    */
  def empty: ConverterMetrics = new ConverterMetrics(new MetricRegistry(), None, "", Seq.empty)

  /**
    * Create a registry for the provided feature type
    *
    * @param sft simple feature type
    * @param reporters configs for metric reporters
    * @return
    */
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
  class SimpleGauge[T] extends Gauge[T] {

    @volatile
    private var value: T = _

    override def getValue: T = value

    def set(value: T): Unit = this.value = value
  }
}
