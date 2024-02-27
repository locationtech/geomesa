/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.client.admin.TableOperations
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope
import org.apache.accumulo.core.iterators.{Filter, IteratorEnvironment, SortedKeyValueIterator}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.index.filters.AgeOffFilter
import org.locationtech.geomesa.utils.conf.FeatureExpiration
import org.locationtech.geomesa.utils.conf.FeatureExpiration.IngestTimeExpiration
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

/**
  * Accumulo implementation of AgeOffFilter, based on an expiry. Features with a timestamp older than
  * the expiry will be aged off.
  *
  * This iterator can be configured on scan, minc, and majc to age off data
  *
  * Age off iterators can be stacked but this may have performance implications
  */
class AgeOffIterator extends Filter with AgeOffFilter {

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: java.util.Map[String, String],
                    env: IteratorEnvironment): Unit = {

    import scala.collection.JavaConverters._

    super.init(source, options, env)
    super.init(options.asScala.toMap)
  }

  override def accept(k: Key, v: Value): Boolean = accept(null, -1, -1, null, -1, -1, k.getTimestamp)

  override def deepCopy(env: IteratorEnvironment): SortedKeyValueIterator[Key, Value] = {
    val copy = super.deepCopy(env).asInstanceOf[AgeOffIterator]
    copy.expiry = expiry
    copy
  }
}

object AgeOffIterator extends LazyLogging {

  val Name = "age-off"

  def configure(sft: SimpleFeatureType, expiry: Duration, priority: Int = 5): IteratorSetting = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    require(!sft.isLogicalTime, "AgeOff iterator will not work with Accumulo logical time - set user data " +
          s"'${SimpleFeatureTypes.Configs.TableLogicalTime}=false' at schema creation")
    val is = new IteratorSetting(priority, Name, classOf[AgeOffIterator])
    org.locationtech.geomesa.index.filters.AgeOffFilter.configure(sft, expiry).foreach {
      case (k, v) => is.addOption(k, v)
    }
    is
  }

  def expiry(tableOps: TableOperations, table: String): Option[FeatureExpiration] = {
    try {
      list(tableOps, table).map { is =>
        val expiry = java.time.Duration.parse(is.getOptions.get(AgeOffFilter.Configuration.ExpiryOpt)).toMillis
        IngestTimeExpiration(Duration(expiry, TimeUnit.MILLISECONDS))
      }
    } catch {
      case NonFatal(e) => logger.error("Error converting iterator settings to FeatureExpiration:", e); None
    }
  }

  def list(tableOps: TableOperations, table: String): Option[IteratorSetting] = {
    import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichIterator
    IteratorScope.values.iterator.flatMap(scope => Option(tableOps.getIteratorSetting(table, Name, scope))).headOption
  }

  def set(tableOps: TableOperations, table: String, sft: SimpleFeatureType, expiry: Duration): Unit =
    tableOps.attachIterator(table, configure(sft, expiry)) // all scopes

  def clear(tableOps: TableOperations, table: String): Unit = {
    if (IteratorScope.values.exists(scope => tableOps.getIteratorSetting(table, Name, scope) != null)) {
      tableOps.removeIterator(table, Name, java.util.EnumSet.allOf(classOf[IteratorScope]))
    }
  }
}
