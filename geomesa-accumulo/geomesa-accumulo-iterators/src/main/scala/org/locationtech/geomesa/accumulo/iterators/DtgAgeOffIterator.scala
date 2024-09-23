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
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.index.filters.{AgeOffFilter, DtgAgeOffFilter}
import org.locationtech.geomesa.utils.conf.FeatureExpiration
import org.locationtech.geomesa.utils.conf.FeatureExpiration.FeatureTimeExpiration

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

/**
  * Age off data based on the dtg value stored in the SimpleFeature
  */
class DtgAgeOffIterator extends AgeOffIterator with DtgAgeOffFilter {

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: java.util.Map[String, String],
                    env: IteratorEnvironment): Unit = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    import scala.collection.JavaConverters._

    super.init(source, options, env)
    try {
      super.init(options.asScala.toMap)
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

object DtgAgeOffIterator extends LazyLogging {

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

  def expiry(tableOps: TableOperations, sft: SimpleFeatureType, table: String): Option[FeatureExpiration] = {
    try {
      list(tableOps, table).map { is =>
        val attribute = sft.getDescriptor(is.getOptions.get(DtgAgeOffFilter.Configuration.DtgOpt).toInt).getLocalName
        val expiry = java.time.Duration.parse(is.getOptions.get(AgeOffFilter.Configuration.ExpiryOpt)).toMillis
        FeatureTimeExpiration(attribute, sft.indexOf(attribute), Duration(expiry, TimeUnit.MILLISECONDS))
      }
    } catch {
      case NonFatal(e) => logger.error("Error converting iterator settings to FeatureExpiration:", e); None
    }
  }

  def list(tableOps: TableOperations, table: String): Option[IteratorSetting] = {
    import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichIterator
    IteratorScope.values.iterator.flatMap(scope => Option(tableOps.getIteratorSetting(table, Name, scope))).headOption
  }

  def set(
      tableOps: TableOperations,
      sft: SimpleFeatureType,
      index: GeoMesaFeatureIndex[_, _],
      expiry: Duration,
      dtg: String): Unit = {
    index.getTableNames().foreach { table =>
      tableOps.attachIterator(table, configure(sft, index, expiry, Option(dtg))) // all scopes
    }
  }

  def clear(tableOps: TableOperations, table: String): Unit = {
    if (IteratorScope.values.exists(scope => tableOps.getIteratorSetting(table, Name, scope) != null)) {
      tableOps.removeIterator(table, Name, java.util.EnumSet.allOf(classOf[IteratorScope]))
    }
  }
}
