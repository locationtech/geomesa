/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope
import org.apache.accumulo.core.iterators.{Filter, IteratorEnvironment, SortedKeyValueIterator}
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.index.filters.AgeOffFilter
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType

import scala.concurrent.duration.Duration

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

    import scala.collection.JavaConversions._

    super.init(source, options, env)
    super.init(options.toMap)
  }

  override def accept(k: Key, v: Value): Boolean = accept(null, -1, -1, null, -1, -1, k.getTimestamp)

  override def deepCopy(env: IteratorEnvironment): SortedKeyValueIterator[Key, Value] = {
    val copy = super.deepCopy(env).asInstanceOf[AgeOffIterator]
    copy.expiry = expiry
    copy
  }
}

object AgeOffIterator {

  val Name = "age-off"

  def configure(sft: SimpleFeatureType, expiry: Duration, priority: Int = 5): IteratorSetting = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    require(!sft.isLogicalTime, "AgeOff iterator will not work with Accumulo logical time - set user data " +
          s"'${SimpleFeatureTypes.Configs.LOGICAL_TIME_KEY}=false' at schema creation")
    val is = new IteratorSetting(priority, Name, classOf[AgeOffIterator])
    org.locationtech.geomesa.index.filters.AgeOffFilter.configure(sft, expiry).foreach {
      case (k, v) => is.addOption(k, v)
    }
    is
  }

  def list(ds: AccumuloDataStore, sft: SimpleFeatureType): Option[String] = {
    import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichIterator
    val tableOps = ds.connector.tableOperations()
    ds.getAllIndexTableNames(sft.getTypeName).iterator.filter(tableOps.exists).flatMap { table =>
      IteratorScope.values.iterator.flatMap(scope => Option(tableOps.getIteratorSetting(table, Name, scope))).headOption
    }.headOption.map(_.toString)
  }

  def set(ds: AccumuloDataStore, sft: SimpleFeatureType, expiry: Duration): Unit = {
    val tableOps = ds.connector.tableOperations()
    ds.getAllIndexTableNames(sft.getTypeName).foreach { table =>
      if (tableOps.exists(table)) {
        tableOps.attachIterator(table, configure(sft, expiry)) // all scopes
      }
    }
  }

  def clear(ds: AccumuloDataStore, sft: SimpleFeatureType): Unit = {
    val tableOps = ds.connector.tableOperations()
    ds.getAllIndexTableNames(sft.getTypeName).filter(tableOps.exists).foreach { table =>
      if (IteratorScope.values.exists(scope => tableOps.getIteratorSetting(table, Name, scope) != null)) {
        tableOps.removeIterator(table, Name, java.util.EnumSet.allOf(classOf[IteratorScope]))
      }
    }
  }
}
