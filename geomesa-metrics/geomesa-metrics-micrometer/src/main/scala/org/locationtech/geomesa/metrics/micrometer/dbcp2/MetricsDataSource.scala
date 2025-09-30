/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.metrics.micrometer.dbcp2

import org.apache.commons.dbcp2._
import org.apache.commons.pool2.impl.{AbandonedConfig, GenericObjectPool, GenericObjectPoolConfig}

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/**
 * Extension of commons-dbcp data source that works with micrometer/prometheus metrics.
 * See https://github.com/micrometer-metrics/micrometer/issues/2593 for reference.
 *
 * We skip the standard jmx exposure in BasicDataSource, because we only want to register the connection pool,
 * otherwise gauges don't register correctly due to jmx name overlap.
 *
 * BasicDataSource registers jmx mbeans in at least 3 places:
 *  - itself, using the `jmxName` set by the user
 *  - its GenericObjectPool, using `config.getJmxNameBase` + `config.getJmxNamePrefix`
 *  - connections created with its PoolableConnectionFactory, using "jmxName" + ",connectionpool=connections,connection=" + `i`
 *
 * Micrometer's CommonsObjectPool2Metrics tries to register anything with the jmx name prefix 'org.apache.commons.pool2:'. It uses
 * the `name` attribute in the jmx name for a tag, and it tries to read `getFactoryType` from the underlying object as a tag.
 * It exposes pool-related values from the jmx objects (e.g. NumIdle, etc) as metrics.
 *
 * The only thing that makes sense to instrument with those values is the GenericObjectPool. Thus, we skip jmx registration for
 * this data source and the underlying connections. Indeed, since the metric name is based on the jmx name, and the metric
 * value is pulled from the object itself, registering the datasource or connections will break the metric since the object
 * doesn't have the expected pool methods.
 *
 * Note: short-lived data sources may cause error logs like `ERROR CommonsObjectPool2Metrics: can not set name tag` due to
 * already being removed from the mbean server by the time the jmx listener fires.
 */
class MetricsDataSource(jmxNamePrefix: String = "dbcp") extends BasicDataSource {

  // in order for gauges to work correctly, we need unique metrics tags based on the name
  // the pool will increment names until it finds one that isn't registered, but if a datasource is closed it will deregister,
  // causing a potential name conflict
  // instead we track them here with an incrementing counter
  val jmxName: String = MetricsDataSource.getUniqueName(jmxNamePrefix)

  override protected def createObjectPool(
      factory: PoolableConnectionFactory,
      poolConfig: GenericObjectPoolConfig[PoolableConnection],
      abandonedConfig: AbandonedConfig): GenericObjectPool[PoolableConnection] = {
    poolConfig.setJmxEnabled(true)
    poolConfig.setJmxNameBase(MetricsDataSource.NameBase)
    poolConfig.setJmxNamePrefix(jmxName)
    super.createObjectPool(factory, poolConfig, abandonedConfig)
  }

  /**
   * Register this pool with JMX in order to expose metrics. Not thread safe.
   */
  @deprecated("Registration happens automatically")
  def registerJmx(): Unit = {}
}

object MetricsDataSource {

  // required for micrometer metrics:
  //  - prefix must be `org.apache.commons.pool2`
  //  - type must be either `GenericObjectPool` or `GenericKeyedObjectPool`
  private val NameBase =
    s"org.apache.commons.pool2:type=GenericObjectPool${Constants.JMX_CONNECTION_POOL_BASE_EXT}${Constants.JMX_CONNECTION_POOL_PREFIX},name="

  private val nameCounters = new ConcurrentHashMap[String, AtomicInteger]()

  private def getUniqueName(base: String): String = {
    val i = nameCounters.computeIfAbsent(base, _ => new AtomicInteger(0)).getAndIncrement()
    if (i == 0) { base } else { s"$base$i" }
  }
}
