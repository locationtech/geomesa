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
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.index.filters.DtgAgeOffFilter
import org.opengis.feature.simple.SimpleFeatureType

import scala.concurrent.duration.Duration

/**
  * Age off data based on the dtg value stored in the SimpleFeature
  */
class DtgAgeOffIterator extends AgeOffIterator with DtgAgeOffFilter {

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: java.util.Map[String, String],
                    env: IteratorEnvironment): Unit = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    import scala.collection.JavaConversions._

    super.init(source, options, env)
    try {
      super.init(options.toMap)
    } catch {
      case _: NoSuchElementException => dtgIndex = sft.getDtgIndex.get // fallback for old configuration
    }
  }

  override def accept(k: Key, v: Value): Boolean = {
    val value = v.get()
    accept(null, -1, -1, value, 0, value.length, -1)
  }

  override def deepCopy(env: IteratorEnvironment): SortedKeyValueIterator[Key, Value] = {
    val copy = super.deepCopy(env).asInstanceOf[DtgAgeOffIterator]
    copy.sft = sft
    copy.index = index
    copy.reusableSf = reusableSf
    copy.dtgIndex = dtgIndex
    copy
  }
}

object DtgAgeOffIterator {

  val Name = "dtg-age-off"

  def configure(sft: SimpleFeatureType,
                index: GeoMesaFeatureIndex[_, _],
                expiry: Duration,
                dtgField: Option[String],
                priority: Int = 5): IteratorSetting = {
    val is = new IteratorSetting(priority, Name, classOf[DtgAgeOffIterator])
    DtgAgeOffFilter.configure(sft, index, expiry, dtgField).foreach { case (k, v) => is.addOption(k, v) }
    is
  }

  def list(ds: AccumuloDataStore, sft: SimpleFeatureType): Option[String] = {
    import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichIterator
    val tableOps = ds.connector.tableOperations()
    ds.getAllIndexTableNames(sft.getTypeName).iterator.filter(tableOps.exists).flatMap { table =>
      IteratorScope.values.iterator.flatMap(scope => Option(tableOps.getIteratorSetting(table, Name, scope))).headOption
    }.headOption.map(_.toString)
  }

  def set(ds: AccumuloDataStore, sft: SimpleFeatureType, expiry: Duration, dtg: String): Unit = {
    val tableOps = ds.connector.tableOperations()
    ds.manager.indices(sft).foreach { index =>
      index.getTableNames(None).foreach { table =>
        if (tableOps.exists(table)) {
          tableOps.attachIterator(table, configure(sft, index, expiry, Option(dtg))) // all scopes
        }
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