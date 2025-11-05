/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.metrics.micrometer

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import io.micrometer.core.instrument.binder.MeterBinder
import io.micrometer.core.instrument.binder.commonspool2.CommonsObjectPool2Metrics
import io.micrometer.core.instrument.binder.jvm.{ClassLoaderMetrics, JvmGcMetrics, JvmMemoryMetrics, JvmThreadMetrics}
import io.micrometer.core.instrument.binder.system.ProcessorMetrics
import io.micrometer.core.instrument.{Metrics, Tag}
import pureconfig.{ConfigReader, ConfigSource}

import java.util.Collections
import java.util.concurrent.ConcurrentHashMap

/**
 * Manages micrometer instrumentation loading
 */
object Instrumentations extends StrictLogging {

  import pureconfig.generic.semiauto._

  import scala.collection.JavaConverters._

  // instrumentations that we've already bound, to prevent duplicates
  private val bindings = Collections.newSetFromMap(new ConcurrentHashMap[(String, Map[String, String]), java.lang.Boolean]())

  private val defaults = ConfigFactory.load("instrumentations")

  /**
   * Configure default instrumentations
   */
  def bind(conf: Config = ConfigFactory.empty()): Unit = {
    val instrumentations = InstrumentationConfig(conf.withFallback(defaults))
    addBinding(instrumentations.classloader, "JVM classloader", tags => new ClassLoaderMetrics(tags))
    addBinding(instrumentations.memory,      "JVM memory",      tags => new JvmMemoryMetrics(tags))
    addBinding(instrumentations.gc,          "JVM gc",          tags => new JvmGcMetrics(tags))
    addBinding(instrumentations.processor,   "JVM processor",   tags => new ProcessorMetrics(tags))
    addBinding(instrumentations.threads,     "JVM thread",      tags => new JvmThreadMetrics(tags))
    addBinding(instrumentations.commonsPool, "commons pool",    tags => new CommonsObjectPool2Metrics(tags))
  }

  /**
   * Add a meter binding
   *
   * @param config config
   * @param key unique key to identify this binding
   * @param binder binder constructor
   */
  private def addBinding(config: Instrumentation, key: String, binder: java.lang.Iterable[Tag] => MeterBinder): Unit = {
    if (config.enabled && bindings.add(key -> config.tags)) {
      val tags = config.tags.map { case (k, v) => Tag.of(k, v) }
      logger.info(s"Adding $key metrics${if (tags.isEmpty) "" else s" with ${tags.toSeq.map(_.toString).sorted.mkString(", ")}"}")
      binder(tags.asJava).bindTo(Metrics.globalRegistry)
    }
  }

  private case class Instrumentation(enabled: Boolean, tags: Map[String, String])

  private case class InstrumentationConfig(
    classloader: Instrumentation,
    memory: Instrumentation,
    gc: Instrumentation,
    processor: Instrumentation,
    threads: Instrumentation,
    commonsPool: Instrumentation,
  )

  private object InstrumentationConfig {
    // noinspection ScalaUnusedSymbol
    def apply(conf: Config): InstrumentationConfig = {
      implicit val r0: ConfigReader[Instrumentation] = deriveReader[Instrumentation]
      implicit val r1: ConfigReader[InstrumentationConfig] = deriveReader[InstrumentationConfig]
      ConfigSource.fromConfig(conf).loadOrThrow[InstrumentationConfig]
    }
  }

}
