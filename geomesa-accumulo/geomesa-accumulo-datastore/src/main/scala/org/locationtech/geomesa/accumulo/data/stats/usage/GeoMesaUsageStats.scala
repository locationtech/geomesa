/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.data.stats.usage

import java.io.Closeable

import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.security.Authorizations
import org.joda.time.Interval

import scala.reflect.ClassTag

trait GeoMesaUsageStats extends Closeable {

  /**
    * Writes a usage stat
    *
    * @param stat stat to write
    * @param transform conversion to mutations
    * @tparam T stat type
    */
  def writeUsageStat[T <: UsageStat](stat: T)(implicit transform: UsageStatTransform[T]): Unit

  /**
    * Retrieves usage statistics
    *
    * @param typeName simple feature type name
    * @param dates dates to retrieve stats for
    * @param auths authorizations to query with
    * @param transform conversion to mutations
    * @tparam T stat type
    * @return iterator of usage stats
    */
  def getUsageStats[T <: UsageStat](typeName: String,
                                    dates: Interval,
                                    auths: Authorizations)
                                   (implicit transform: UsageStatTransform[T]): Iterator[T]
}

trait HasGeoMesaUsageStats {
  def usageStats: GeoMesaUsageStats
}

class GeoMesaUsageStatsImpl(connector: Connector, usageStatsTable: String, collectUsageStats: Boolean)
    extends GeoMesaUsageStats {

  private val usageWriter = if (collectUsageStats) new UsageStatWriter(connector, usageStatsTable) else null
  private val usageReader = new UsageStatReader(connector, usageStatsTable)

  override def writeUsageStat[T <: UsageStat](stat: T)(implicit transform: UsageStatTransform[T]): Unit =
    if (usageWriter != null) { usageWriter.queueStat(stat)(transform) }

  override def getUsageStats[T <: UsageStat](typeName: String,
                                             dates: Interval,
                                             auths: Authorizations)
                                            (implicit transform: UsageStatTransform[T]): Iterator[T] =
    usageReader.query(typeName, dates, auths)(transform)

  override def close(): Unit = if (usageWriter != null) { usageWriter.close() }
}
