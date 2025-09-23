/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.metrics.micrometer

import com.typesafe.config.{Config, ConfigFactory}
import io.micrometer.core.instrument.{MeterRegistry, Metrics}

import java.io.Closeable
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import scala.util.Try

/**
 * Factory for creating registries from config
 *
 * @param name Well-known name of this registry
 */
abstract class RegistryFactory(val name: String) {

  private val registrations = new ConcurrentHashMap[Config, RegistryHolder]()

  private val default: Config = ConfigFactory.load(name)

  /**
   * Create a registry using the default configuration
   *
   * @return
   */
  def apply(): MeterRegistry = apply(default)

  /**
   * Create a registry
   *
   * @param conf config
   * @return
   */
  def apply(conf: Config): MeterRegistry = createRegistry(if (conf.eq(default)) { conf } else { conf.withFallback(default) })

  /**
   * Register with this instance, which will ensure the registry managed by this instance
   * is added to the global metrics registry, until the returned object is closed
   */
  def register(): Closeable = register(default)

  /**
   * Register using the given config
   *
   * @param conf registry config
   * @return
   */
  def register(conf: Config): Closeable =
    registrations.computeIfAbsent(if (conf.eq(default)) { conf } else { conf.withFallback(default) }, new RegistryHolder(_)).register()

  /**
   * Create a new registry
   *
   * @param config conf
   * @return
   */
  protected def createRegistry(config: Config): MeterRegistry

  /**
   * Holds references to a registry
   *
   * @param conf registry config
   */
  private class RegistryHolder(conf: Config) {

    private var registry: MeterRegistry = _
    private var count = 0

    /**
     * Register a consumer of the registry held by this object
     *
     * @return a handle to the registration, call `close()` to deregister
     */
    def register(): Closeable = synchronized {
      if (count == 0) {
        registry = createRegistry(conf)
        Metrics.addRegistry(registry)
        Instrumentations.bind(conf)
      }
      count += 1
      new Deregistration()
    }

    /**
     * Deregister the lock, called internally when a registration is closed
     */
    private def deregister(): Unit = synchronized {
      if (count == 1) {
        Metrics.removeRegistry(registry)
        Try(registry.close())
        registry = null
      }
      count -= 1
    }

    /**
     * Registration holder that ensures only a single `deregister` is called per `register`
     */
    private class Deregistration extends Closeable {

      private val closed = new AtomicBoolean(false)

      override def close(): Unit = {
        // ensure we only deregister once
        if (closed.compareAndSet(false, true)) {
          deregister()
        }
      }
    }
  }
}

object RegistryFactory {
  // identifiers for our registry factories
  // note that we keep these here to avoid a runtime dependency on any unused registries
  val Cloudwatch = "cloudwatch"
  val Prometheus = "prometheus"
}
