/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.metrics.core

import java.io.Closeable

import com.codahale.metrics.MetricRegistry.MetricSupplier
import com.codahale.metrics._
import com.typesafe.config.Config
import org.locationtech.geomesa.metrics.core.GeoMesaMetrics.SimpleGauge
import org.locationtech.geomesa.utils.io.CloseWithLogging

/**
  * Provides namespaced access to reporting metrics
  *
  * @param registry metric registry
  * @param prefix namespace prefix
  * @param reporters metric reporters
  */
class GeoMesaMetrics(val registry: MetricRegistry, prefix: String, reporters: Seq[ScheduledReporter])
    extends Closeable {

  private val pre = GeoMesaMetrics.safePrefix(prefix)

  protected def id(typeName: String, id: String): String = s"$pre${GeoMesaMetrics.safePrefix(typeName)}$id"

  /**
   * Creates a prefixed counter
   *
   * @param typeName simple feature type name
   * @param id short identifier for the metric being counted
   * @return
   */
  def counter(typeName: String, id: String): Counter = registry.counter(this.id(typeName, id))

  /**
   * Gets an updatable gauge
   *
   * @param typeName simple feature type name
   * @param id short identifier for hte metric being gauged
   * @tparam T gauge type
   * @return
   */
  def gauge[T](typeName: String, id: String): SimpleGauge[T] =
    registry.gauge(this.id(typeName, id), GeoMesaMetrics.GaugeSupplier).asInstanceOf[SimpleGauge[T]]

  /**
   * Creates a prefixed histogram
   *
   * @param typeName simple feature type name
   * @param id short identifier for the metric being histogramed
   * @return
   */
  def histogram(typeName: String, id: String): Histogram = registry.histogram(this.id(typeName, id))

  /**
   * Creates a prefixed meter
   *
   * @param typeName simple feature type name
   * @param id short identifier for the metric being metered
   * @return
   */
  def meter(typeName: String, id: String): Meter = registry.meter(this.id(typeName, id))

  /**
   * Creates a prefixed timer
   *
   * @param typeName simple feature type name
   * @param id short identifier for the metric being timed
   * @return
   */
  def timer(typeName: String, id: String): Timer = registry.timer(this.id(typeName, id))

  /**
   * Register a metric
   *
   * @param typeName simple feature type name
   * @param id short identifier for the metric
   * @param metric metric to register
   * @tparam T metric type
   * @return
   */
  def register[T <: Metric](typeName: String, id: String, metric: T): T =
    registry.register(this.id(typeName, id), metric)

  override def close(): Unit = CloseWithLogging(reporters)
}

object GeoMesaMetrics {

  private val GaugeSupplier = new MetricSupplier[Gauge[_]] {
    override def newMetric(): Gauge[_] = new SimpleGauge()
  }

  /**
    * Create a registry
    *
    * @param prefix metric name prefix
    * @param reporters configs for metric reporters
    * @return
    */
  def apply(prefix: String, reporters: Seq[Config]): GeoMesaMetrics = {
    val registry = new MetricRegistry()
    val reps = reporters.map(ReporterFactory.apply(_, registry)).toList
    new GeoMesaMetrics(registry, prefix, reps)
  }

  private def safePrefix(name: String): String = {
    val replaced = name.replaceAll("[^A-Za-z0-9]", ".")
    if (replaced.isEmpty || replaced.endsWith(".")) { replaced } else { s"$replaced." }
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
