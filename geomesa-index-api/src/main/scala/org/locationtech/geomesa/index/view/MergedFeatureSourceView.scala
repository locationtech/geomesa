/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.index.view

import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.data._
import org.geotools.api.feature.FeatureVisitor
import org.geotools.api.feature.`type`.Name
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.geotools.api.filter.sort.SortBy
import org.geotools.api.util.ProgressListener
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.filter.andFilters
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureCollection.GeoMesaFeatureVisitingCollection
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureSource.DelegatingResourceInfo
import org.locationtech.geomesa.index.process.GeoMesaProcessVisitor
import org.locationtech.geomesa.index.view.MergedDataStoreView.MergedSource
import org.locationtech.geomesa.index.view.MergedFeatureSourceView.MergedQueryCapabilities
import org.locationtech.geomesa.index.view.MergedFilterProvider.QueryContext
import org.locationtech.geomesa.utils.concurrent.CachedThreadPool

import java.awt.RenderingHints.Key
import java.time.Instant
import java.util.Collections
import java.util.concurrent.CopyOnWriteArrayList

/**
 * Feature source for merged data store view
 *
 * @param ds data store
 * @param sources delegate feature sources
 * @param parallel scan stores in parallel (vs sequentially)
 * @param sft simple feature type
 */
class MergedFeatureSourceView(
    ds: MergedDataStoreView,
    sources: Seq[MergedSource[SimpleFeatureSource]],
    parallel: Boolean,
    sft: SimpleFeatureType
  ) extends SimpleFeatureSource with LazyLogging {

  import scala.collection.JavaConverters._

  lazy private val hints = Collections.unmodifiableSet(Collections.emptySet[Key])

  lazy private val capabilities = new MergedQueryCapabilities(sources.map(_.source.getQueryCapabilities))

  override def getSchema: SimpleFeatureType = sft

  override def getCount(query: Query): Int = {
    val context = QueryContext(query.getFilter)
    val total =
      if (parallel) {
        def getSingle(source: MergedSource[SimpleFeatureSource]): Int = source.source.getCount(source.filter(query, context))

        val results = new CopyOnWriteArrayList[Int]()
        sources.toList.map(s => CachedThreadPool.submit(s.decorator(() => results.add(getSingle(s))))).foreach(_.get)
        results.asScala.foldLeft(0)((sum, count) => if (sum < 0 || count < 0) { -1 } else { sum + count })
      } else {
        // if one of our sources can't get a count (i.e. is negative), give up and return -1
        sources.foldLeft(0) { case (sum, source) =>
          lazy val count = source.source.getCount(source.filter(query, context))
          if (sum < 0 || count < 0) { -1 } else { sum + count }
        }
      }
    if (query.isMaxFeaturesUnlimited) {
      total
    } else {
      math.min(total, query.getMaxFeatures)
    }
  }

  override def getBounds: ReferencedEnvelope = getBounds(new Query(sft.getTypeName))

  override def getBounds(query: Query): ReferencedEnvelope = {
    val context = QueryContext(query.getFilter)
    def getSingle(source: MergedSource[SimpleFeatureSource]): Option[ReferencedEnvelope] =
      Option(source.source.getBounds(source.filter(query, context)))

    val sourceBounds = if (parallel) {
      val results = new CopyOnWriteArrayList[ReferencedEnvelope]()
      sources.toList.map(s => CachedThreadPool.submit(s.decorator(() => getSingle(s).foreach(results.add)))).foreach(_.get)
      results.asScala
    } else {
      sources.flatMap(getSingle)
    }

    val bounds = new ReferencedEnvelope(org.locationtech.geomesa.utils.geotools.CRS_EPSG_4326)
    sourceBounds.foreach(bounds.expandToInclude)
    bounds
  }

  override def getQueryCapabilities: QueryCapabilities = capabilities

  override def getFeatures: SimpleFeatureCollection = getFeatures(Filter.INCLUDE)

  override def getFeatures(filter: Filter): SimpleFeatureCollection = getFeatures(new Query(sft.getTypeName, filter))

  override def getFeatures(query: Query): SimpleFeatureCollection = new MergedFeatureCollection(query)

  override def getName: Name = getSchema.getName

  override def getDataStore: DataStore = ds

  override def getSupportedHints: java.util.Set[Key] = hints

  override def getInfo: ResourceInfo = new DelegatingResourceInfo(this)

  override def addFeatureListener(listener: FeatureListener): Unit = throw new UnsupportedOperationException()

  override def removeFeatureListener(listener: FeatureListener): Unit = throw new UnsupportedOperationException()

  /**
    * Feature collection implementation
    *
    * @param query query
    */
  private class MergedFeatureCollection(query: Query)
      extends GeoMesaFeatureVisitingCollection(MergedFeatureSourceView.this, ds.stats, query) {

    private lazy val featureReader = ds.getFeatureReader(sft, Transaction.AUTO_COMMIT, query)

    override def getSchema: SimpleFeatureType = featureReader.schema

    override def reader(): FeatureReader[SimpleFeatureType, SimpleFeature] = featureReader.reader()

    override def getBounds: ReferencedEnvelope = MergedFeatureSourceView.this.getBounds(query)

    override def getCount: Int = MergedFeatureSourceView.this.getCount(query)

    override def size: Int = {
      // note: we shouldn't return -1 here, but we don't return the actual value unless EXACT_COUNT is set
      math.max(0, getCount)
    }

    override def subCollection(filter: Filter): SimpleFeatureCollection = {
      val copy = new Query(query)
      copy.setFilter(andFilters(Seq(copy.getFilter, filter)))
      new MergedFeatureCollection(copy)
    }

    override def sort(order: SortBy): SimpleFeatureCollection = {
      val copy = new Query(query)
      copy.setSortBy(Option(copy.getSortBy).fold(Array(order))(_ :+ order): _*)
      new MergedFeatureCollection(copy)
    }

    override def accepts(visitor: FeatureVisitor, progress: ProgressListener): Unit = visitor match {
      case v: GeoMesaProcessVisitor => v.execute(MergedFeatureSourceView.this, new Query(query))
      case _ => super.accepts(visitor, progress)
    }
  }
}

object MergedFeatureSourceView {

  /**
    * Query capabilities
    *
    * @param capabilities delegates
    */
  class MergedQueryCapabilities(capabilities: Seq[QueryCapabilities]) extends QueryCapabilities {
    override def isOffsetSupported: Boolean = capabilities.forall(_.isOffsetSupported)
    override def supportsSorting(sortAttributes: SortBy*): Boolean =
      capabilities.forall(_.supportsSorting(sortAttributes: _*))
    override def isReliableFIDSupported: Boolean = capabilities.forall(_.isReliableFIDSupported)
    override def isUseProvidedFIDSupported: Boolean = capabilities.forall(_.isUseProvidedFIDSupported)
    override def isJoiningSupported: Boolean = capabilities.forall(_.isJoiningSupported)
    override def isVersionSupported: Boolean = capabilities.forall(_.isVersionSupported)
  }
}
