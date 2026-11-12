/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.index.view

import org.geotools.api.data.DataStore
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.filter.ff
import org.locationtech.geomesa.index.view.MergedDataStoreView.{FilterProvider, MergedSource}
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

import java.time.Instant
import java.util.Date

/**
 * Route a certified interval to the destination, and its complement (including null dates) to the source.
 * Dataset IDs are feature type names. Each type may occur in at most one non-destination delegate.
 * The merged view owns this provider, its coverage provider and its physical stores.
 */
class CoverageFilterProvider(destination: DataStore, coverage: DestinationCoverage) extends FilterProvider {

  import scala.collection.JavaConverters._

  override def validate(stores: Seq[MergedSource[DataStore]]): Unit = {
    require(stores.count(_.source eq destination) == 1, "Configure the destination store exactly once")
    require(stores.exists(s => !(s.source eq destination)), "Configure at least one source store")
  }

  override def select(typeName: String, stores: Seq[MergedSource[DataStore]]): Seq[MergedSource[DataStore]] = {
    val selected = stores.filter(_.source.getTypeNames.contains(typeName))
    require(selected.count(s => !(s.source eq destination)) <= 1,
      s"More than one source store advertises '$typeName'")
    if (selected.size > 1) { dateField(typeName, selected) }
    selected
  }

  override def filters(typeName: String, stores: Seq[MergedSource[DataStore]], now: Instant): Seq[Filter] = {
    // Consult coverage even if destination metadata disappears: certified data must not silently fall back to source.
    coverage.coverage(typeName) match {
      case None =>
        require(stores.exists(s => !(s.source eq destination)),
          s"No published destination coverage or source store for '$typeName'")
        stores.map(s => if (s.source eq destination) { Filter.EXCLUDE } else { Filter.INCLUDE })
      case Some(interval) =>
        require(stores.exists(_.source eq destination),
          s"Published coverage for '$typeName' has no available destination schema")
        val dtg = ff.property(dateField(typeName, stores))
        val start = ff.literal(Date.from(interval.from))
        val end = ff.literal(Date.from(interval.through))
        val inside = ff.and(ff.greaterOrEqual(dtg, start), ff.less(dtg, end))
        val outside = ff.or(ff.isNull(dtg), ff.or(ff.less(dtg, start), ff.greaterOrEqual(dtg, end)))
        stores.map(s => if (s.source eq destination) { inside } else { outside })
    }
  }

  override def close(): Unit = coverage.close()

  private def dateField(typeName: String, stores: Seq[MergedSource[DataStore]]): String = {
    val fields = stores.map { store =>
      val schema = store.source.getSchema(typeName)
      require(schema != null, s"Missing advertised schema '$typeName'")
      val field = schema.getDtgField.orElse {
        schema.getAttributeDescriptors.asScala.collectFirst {
          case d if classOf[Date].isAssignableFrom(d.getType.getBinding) => d.getLocalName
        }
      }.getOrElse(throw new IllegalArgumentException(s"Schema '$typeName' has no temporal attribute"))
      require(Option(schema.getDescriptor(field)).exists(d => classOf[Date].isAssignableFrom(d.getType.getBinding)),
        s"Schema '$typeName' has an invalid temporal attribute '$field'")
      field
    }
    require(fields.distinct.size == 1,
      s"Delegates for '$typeName' use different temporal attributes: ${fields.mkString(", ")}")
    fields.head
  }
}
