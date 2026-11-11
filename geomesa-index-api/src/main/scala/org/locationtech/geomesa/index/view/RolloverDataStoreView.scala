/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.index.view

import org.geotools.api.data._
import org.geotools.api.feature.`type`.Name
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.geotools.feature.NameImpl
import org.locationtech.geomesa.filter.ff
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureReader
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureReader.HasGeoMesaFeatureReader
import org.locationtech.geomesa.index.stats.{GeoMesaStats, HasGeoMesaStats}
import org.locationtech.geomesa.index.view.MergedDataStoreView.{DateFilter, MergedSource}
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.index.view.MergedDataStoreView.MergedSource.TypeOption
import org.locationtech.geomesa.index.view.RolloverDataStoreView.Delegate

import java.util.Date
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.UnaryOperator

/**
 * Read-only rollover between existing stores using producer-certified destination coverage.
 * Delegates are borrowed and remain owned by the caller. This view owns and closes the coverage provider.
 * Each query operation captures one interval; cached feature sources resolve fresh coverage for subsequent operations.
 * Source rows outside the interval remain queryable, independently of source retention policies.
 *
 * @param sources source delegates; a feature type may be advertised by at most one
 * @param destination destination delegate
 * @param coverage provider of complete, queryable and final destination intervals, keyed by feature type
 * @param parallel scan delegates concurrently
 * @param taskDecorator caller-supplied context propagation for parallel operations
 * @param namespace namespace of this logical view
 */
class RolloverDataStoreView(
    sources: Seq[Delegate],
    destination: Delegate,
    coverage: RolloverCoverage,
    parallel: Boolean = false,
    taskDecorator: Option[UnaryOperator[Runnable]] = None,
    namespace: Option[String] = None
  ) extends ReadOnlyDataStore with HasGeoMesaFeatureReader with HasGeoMesaStats {

  require(sources.nonEmpty, "Configure at least one source store")
  require(!sources.exists(_.dataStore eq destination.dataStore), "Source and destination stores must differ")

  import scala.collection.JavaConverters._

  private val delegates = sources :+ destination
  private val disposed = new AtomicBoolean(false)

  override def getTypeNames: Array[String] =
    delegates.flatMap(_.typeNames).distinct.sorted.toArray

  override def getNames: java.util.List[Name] =
    getTypeNames.map(n => new NameImpl(namespace.orNull, n): Name).toList.asJava

  override def getSchema(name: Name): SimpleFeatureType = getSchema(name.getLocalPart)

  override def getSchema(typeName: String): SimpleFeatureType = {
    val selected = metadata(typeName)
    selected.map(_.getSchema(typeName)).orNull
  }

  override def getFeatureReader(query: Query, transaction: Transaction): SimpleFeatureReader = {
    val copy = new Query(query)
    snapshot(copy.getTypeName).getFeatureReader(copy, transaction)
  }

  override private[geomesa] def getFeatureReader(
      sft: SimpleFeatureType, transaction: Transaction, query: Query): GeoMesaFeatureReader = {
    require(sft.getTypeName == query.getTypeName, "Feature type and query must agree")
    val copy = new Query(query)
    val view = snapshot(copy.getTypeName)
    view.getFeatureReader(view.getSchema(copy.getTypeName), transaction, copy)
  }

  override val stats: GeoMesaStats = new RolloverStats(snapshot)

  override def getFeatureSource(typeName: String): SimpleFeatureSource = {
    require(getSchema(typeName) != null, s"Unknown rollover feature type '$typeName'")
    new RolloverFeatureSourceView(this, typeName, () => snapshot(typeName), () => getSchema(typeName),
      () => metadata(typeName).get.getFeatureSource(typeName).getQueryCapabilities)
  }
  override def getFeatureSource(name: Name): SimpleFeatureSource = getFeatureSource(name.getLocalPart)
  override def getInfo: ServiceInfo = destination.dataStore.getInfo
  override def getLockingManager: LockingManager = null
  override def dispose(): Unit = if (disposed.compareAndSet(false, true)) { coverage.close() }

  private[view] def ensureOpen(): Unit = require(!disposed.get(), "Rollover view has been disposed")

  private def metadata(typeName: String): Option[MergedDataStoreView] = {
    val (source, target) = select(typeName)
    val selected = source.toSeq ++ target.toSeq
    if (selected.isEmpty) { None } else {
      if (selected.size > 1) { validateDateFields(typeName, selected) }
      Some(merged(selected.map(s => MergedSource(s.dataStore))))
    }
  }

  private def validateDateFields(typeName: String, selected: Seq[Delegate]): String = {
    val fields = selected.map { delegate =>
      val schema = delegate.dataStore.getSchema(typeName)
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
    require(fields.distinct.size == 1, s"Rollover delegates for '$typeName' use different temporal attributes: ${fields.mkString(", ")}")
    fields.head
  }

  private def select(typeName: String): (Option[Delegate], Option[Delegate]) = {
    ensureOpen()
    require(typeName != null, "A rollover feature type is required")
    def contains(store: Delegate): Boolean = store.typeNames.contains(typeName)
    val selected = sources.filter(contains)
    require(selected.size <= 1, s"More than one source store advertises '$typeName'")
    (selected.headOption, Option(destination).filter(contains))
  }

  private def snapshot(typeName: String): MergedDataStoreView = {
    val (source, target) = select(typeName)
    require(source.isDefined || target.isDefined, s"Unknown rollover feature type '$typeName'")
    // Consult coverage even if the destination stops advertising the type. Missing
    // metadata must not turn a certified destination interval into a source-only response.
    val selected = coverage.coverage(typeName) match {
      case None =>
        require(source.isDefined, s"No published destination coverage or source store for '$typeName'")
        source.toSeq.map(s => MergedSource(s.dataStore))
      case Some(interval) =>
        require(target.isDefined, s"Published coverage for '$typeName' has no available destination schema")
        val dtg = validateDateFields(typeName, source.toSeq ++ target.toSeq)
        val start = TypeOption[DateFilter](DateFilter.StaticDate(Date.from(interval.from)))
        val end = TypeOption[DateFilter](DateFilter.StaticDate(Date.from(interval.through)))
        // A null timestamp cannot belong to a certified temporal interval and remains source-owned.
        val before = TypeOption[Filter](ff.or(ff.isNull(ff.property(dtg)),
          ff.less(ff.property(dtg), ff.literal(Date.from(interval.from)))))
        source.toSeq.flatMap(s => Seq(MergedSource(s.dataStore).copy(sourceFilter = before), MergedSource(s.dataStore).copy(from = end))) :+
          MergedSource(target.get.dataStore).copy(from = start, to = end)
    }
    merged(selected)
  }

  private def merged(selected: Seq[MergedSource[DataStore]]): MergedDataStoreView =
    new MergedDataStoreView(selected, deduplicate = false, parallel = parallel,
      taskDecorator = taskDecorator, namespace = namespace) {
      // Operation plans borrow delegates just like the public view. They are kept
      // private, but must never acquire ownership if their lifecycle changes later.
      override def dispose(): Unit = {}
    }
}

object RolloverDataStoreView {
  /** A borrowed datastore with an optional feature-type selection predicate, applied to discovery and routing. */
  case class Delegate(dataStore: DataStore, acceptsType: String => Boolean = _ => true) {
    private[view] def typeNames: Seq[String] = dataStore.getTypeNames.filter(acceptsType).toSeq
  }
}
