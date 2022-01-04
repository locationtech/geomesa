/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
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
   * Gets a gauge. Note that it is possible (although unlikely) that the gauge will not be the
   * one from the supplier, if the id has already been registered
   *
   * @param typeName simple feature type name
   * @param id short identifier for the metric being gauged
   * @param supplier metric supplier
   * @return
   */
  def gauge(typeName: String, id: String, supplier: MetricSupplier[Gauge[_]]): Gauge[_] =
    registry.gauge(this.id(typeName, id), supplier)

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
   * Register a metric. Note that in comparison to most methods in this class, a given identifier
   * can only be registered once
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
}
