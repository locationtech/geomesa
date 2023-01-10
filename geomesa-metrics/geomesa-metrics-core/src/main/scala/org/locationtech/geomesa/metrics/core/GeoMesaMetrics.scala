/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.metrics.core

<<<<<<< HEAD
=======
import com.codahale.metrics.MetricRegistry.MetricSupplier
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
import com.codahale.metrics._
import com.typesafe.config.Config
import org.locationtech.geomesa.utils.io.CloseWithLogging

import java.io.Closeable

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
  def gauge(typeName: String, id: String, metric: => Gauge[_]): Gauge[_] = {
    val ident = this.id(typeName, id)
    // note: don't use MetricRegistry#gauge(String, MetricSupplier<Gauge>) to support older
    // metric jars that ship with hbase
    def getOrCreate(): Gauge[_] = {
      registry.getMetrics.get(ident) match {
        case g: Gauge[_] => g
        case null => registry.register(ident, metric)
        case m =>
          throw new IllegalArgumentException(s"${m.getClass.getSimpleName} already registered under the name '$ident'")
      }
    }

    // re-try once to avoid concurrency issues with checking then adding a metric (which should be rare)
    try { getOrCreate() } catch { case _: IllegalArgumentException => getOrCreate() }
  }

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
