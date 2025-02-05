/***********************************************************************
 * Copyright (c) 2013-2025 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.metrics.micrometer.dbcp2

import org.locationtech.geomesa.metrics.micrometer.dbcp2.MetricsDataSource.MetricsDataSourceMXBean
import org.apache.commons.dbcp2.{BasicDataSource, Constants, DataSourceMXBean, PoolableConnection, PoolableConnectionFactory}
import org.apache.commons.pool2.impl.{AbandonedConfig, GenericObjectPool, GenericObjectPoolConfig}

import java.lang.management.ManagementFactory
import javax.management.{ObjectName, StandardMBean}

/**
 * Extension of commons-dbcp data source that works with micrometer/prometheus metrics.
 * See https://github.com/micrometer-metrics/micrometer/issues/2593 for reference.
 *
 * We skip the standard jmx exposure in BasicDataSource (triggered by using setJmxName),
 * because we need to register the bean using our custom `MetricsDataSourceMXBean` in order
 * to expose FactoryType.
 */
class MetricsDataSource extends BasicDataSource with MetricsDataSourceMXBean {

  @volatile
  private var registered: Boolean = false

  // required for micrometer metrics -
  // prefix must be `org.apache.commons.pool2`, type must be either `GenericObjectPool` or `GenericKeyedObjectPool`
  private val name = new ObjectName("org.apache.commons.pool2:name=dbcp,type=GenericObjectPool")

  // needs to be this string in order to avoid creating empty metrics that don't get updated
  override def getFactoryType: String =
    "org.apache.commons.dbcp2.PoolableConnectionFactory<org.apache.commons.dbcp2.PoolableConnection>"

  override def createObjectPool(
      factory: PoolableConnectionFactory,
      poolConfig: GenericObjectPoolConfig[PoolableConnection],
      abandonedConfig: AbandonedConfig): GenericObjectPool[PoolableConnection] = {
    if (registered) {
      // taken from org.apache.commons.dbcp2.BasicDataSource#updateJmxName
      poolConfig.setJmxNameBase(s"$name${Constants.JMX_CONNECTION_POOL_BASE_EXT}")
      poolConfig.setJmxNamePrefix(Constants.JMX_CONNECTION_POOL_PREFIX)
      poolConfig.setJmxEnabled(true)
    }
    super.createObjectPool(factory, poolConfig, abandonedConfig)
  }

  /**
   * Register this pool with JMX in order to expose metrics. Not thread safe.
   */
  def registerJmx(): Unit = {
    if (!registered) {
      val mbean = new StandardMBean(this, classOf[MetricsDataSourceMXBean])
      ManagementFactory.getPlatformMBeanServer.registerMBean(mbean, name)
      registered = true
    }
  }

  override def close(): Unit = {
    try {
      if (registered) {
        ManagementFactory.getPlatformMBeanServer.unregisterMBean(name)
      }
    } finally {
      super.close()
    }
  }
}

object MetricsDataSource {
  trait MetricsDataSourceMXBean extends DataSourceMXBean {
    def getFactoryType: String
  }
}
