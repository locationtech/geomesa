/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.metrics.micrometer

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import io.micrometer.core.instrument.{MeterRegistry, Metrics}
import org.locationtech.geomesa.metrics.micrometer.RegistrySetup.RegistryHolder

import java.io.Closeable
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import scala.util.Try

/**
 * Class for configuring metrics
 *
 * @param name name of registry, and associated resource file
 */
class RegistrySetup(val name: String) extends StrictLogging {

  private val registrations = new ConcurrentHashMap[Config, RegistryHolder]()

  protected val registry: Config = ConfigFactory.load(name)

  /**
   * Register with this instance, which will ensure the registry managed by this instance
   * is added to the global metrics registry, until the returned object is closed
   */
  def register(): Closeable = register(registry)

  /**
   * Register using the given config
   *
   * @param conf registry config
   * @return
   */
  protected def register(conf: Config): Closeable = registrations.computeIfAbsent(conf, c => new RegistryHolder(c)).register()

  /**
   * Configure default metrics. Metrics can be customized with environment variables, or with an application.conf,
   * see `<name>.conf` for details
   */
  def configure(): Unit = {
    val instrumentations = ConfigFactory.load("instrumentations")
    if (instrumentations.getBoolean("enabled")) {
      logger.info(s"Configuring $name metrics")
      val registration = register()
      sys.addShutdownHook(registration.close())
      MicrometerSetup.configure(instrumentations.atPath(MicrometerSetup.ConfigPath).withFallback(ConfigFactory.load()))
    } else {
      logger.info(s"$name metrics disabled")
    }
  }
}

object RegistrySetup {

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
        registry = MicrometerSetup.createRegistry(conf)
        Metrics.addRegistry(registry)
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
          RegistryHolder.this.deregister()
        }
      }
    }
  }
}
