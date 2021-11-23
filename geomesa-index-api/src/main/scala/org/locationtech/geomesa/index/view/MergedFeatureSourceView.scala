/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.view

import java.awt.RenderingHints.Key
import java.util.Collections
import java.util.concurrent.atomic.AtomicBoolean

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data._
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureSource}
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureCollection.GeoMesaFeatureVisitingCollection
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureSource.DelegatingResourceInfo
import org.locationtech.geomesa.index.planning.QueryPlanner
import org.locationtech.geomesa.index.view.MergedFeatureSourceView.MergedQueryCapabilities
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
import org.opengis.filter.sort.SortBy

/**
  * Feature source for merged data store view
  *
  * @param ds data store
  * @param sources delegate feature sources
 *  @param parallel scan stores in parallel (vs sequentially)
  * @param sft simple feature type
  */
class MergedFeatureSourceView(
    ds: MergedDataStoreView,
    sources: Seq[(SimpleFeatureSource, Option[Filter])],
    parallel: Boolean,
    sft: SimpleFeatureType
  ) extends SimpleFeatureSource with LazyLogging {

  lazy private val hints = Collections.unmodifiableSet(Collections.emptySet[Key])

  lazy private val capabilities = new MergedQueryCapabilities(sources.map(_._1.getQueryCapabilities))

  override def getSchema: SimpleFeatureType = sft

  override def getCount(query: Query): Int = {
    val total =
      if (parallel) {
        val counts = sources.par.map { case (source, f) => source.getCount(mergeFilter(sft, query, f)) }
        counts.foldLeft(0)((sum, count) => if (sum < 0 || count < 0) { -1 } else { sum + count })
      } else {
        // if one of our sources can't get a count (i.e. is negative), give up and return -1
        sources.foldLeft(0) { case (sum, (source, filter)) =>
          lazy val count = source.getCount(mergeFilter(sft, query, filter))
          if (sum < 0 || count < 0) { -1 } else { sum + count }
        }
      }
    if (query.isMaxFeaturesUnlimited) {
      total
    } else {
      math.min(total, query.getMaxFeatures)
    }
  }

  override def getBounds: ReferencedEnvelope = {
    def getSingle(sourceAndFilter: (SimpleFeatureSource, Option[Filter])): Option[ReferencedEnvelope] = {
      sourceAndFilter match {
        case (source, None)    => Option(source.getBounds)
        case (source, Some(f)) => Option(source.getBounds(new Query(sft.getTypeName, f)))
      }
    }

    val sourceBounds = if (parallel) { sources.par.flatMap(getSingle).seq } else { sources.flatMap(getSingle) }

    val bounds = new ReferencedEnvelope(org.locationtech.geomesa.utils.geotools.CRS_EPSG_4326)
    sourceBounds.foreach(bounds.expandToInclude)
    bounds
  }

  override def getBounds(query: Query): ReferencedEnvelope = {
    def getSingle(sourceAndFilter: (SimpleFeatureSource, Option[Filter])): Option[ReferencedEnvelope] =
      Option(sourceAndFilter._1.getBounds(mergeFilter(sft, query, sourceAndFilter._2)))

    val sourceBounds = if (parallel) { sources.par.flatMap(getSingle).seq } else { sources.flatMap(getSingle) }

    val bounds = new ReferencedEnvelope(org.locationtech.geomesa.utils.geotools.CRS_EPSG_4326)
<<<<<<< HEAD
    sourceBounds.foreach(bounds.expandToInclude)
=======
    sources.foreach {
      case (source, filter) =>
        val source_bounds = source.getBounds(mergeFilter(query, filter))
        if(source_bounds != null){
          bounds.expandToInclude(source_bounds)
        }
    }
>>>>>>> 22da407b4 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
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

  override def addFeatureListener(listener: FeatureListener): Unit = throw new NotImplementedError()

  override def removeFeatureListener(listener: FeatureListener): Unit = throw new NotImplementedError()

  /**
    * Feature collection implementation
    *
    * @param query query
    */
  class MergedFeatureCollection(query: Query)
      extends GeoMesaFeatureVisitingCollection(MergedFeatureSourceView.this, ds.stats, query) {

    private val open = new AtomicBoolean(false)

    override def getSchema: SimpleFeatureType = {
      if (!open.get) {
        // once opened the query will already be configured by the query planner,
        // otherwise we have to compute it here
        ds.runner.configureQuery(sft, query)
      }
      // copy the query so that we don't set any hints for transforms, etc
      val copy = new Query(query)
      copy.setHints(new Hints(query.getHints))
      QueryPlanner.setQueryTransforms(sft, copy)
      ds.runner.getReturnSft(sft, copy.getHints)
    }

    override protected def openIterator(): java.util.Iterator[SimpleFeature] = {
      val iter = super.openIterator()
      open.set(true)
      iter
    }

    override def reader(): FeatureReader[SimpleFeatureType, SimpleFeature] =
      ds.getFeatureReader(query, Transaction.AUTO_COMMIT)

    override def getBounds: ReferencedEnvelope = MergedFeatureSourceView.this.getBounds(query)

    override def getCount: Int = MergedFeatureSourceView.this.getCount(query)

    override def size: Int = {
      // note: we shouldn't return -1 here, but we don't return the actual value unless EXACT_COUNT is set
      val count = getCount
      if (count < 0) { 0 } else { count }
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
    override def supportsSorting(sortAttributes: Array[SortBy]): Boolean =
      capabilities.forall(_.supportsSorting(sortAttributes))
    override def isReliableFIDSupported: Boolean = capabilities.forall(_.isReliableFIDSupported)
    override def isUseProvidedFIDSupported: Boolean = capabilities.forall(_.isUseProvidedFIDSupported)
    override def isJoiningSupported: Boolean = capabilities.forall(_.isJoiningSupported)
    override def isVersionSupported: Boolean = capabilities.forall(_.isVersionSupported)
  }
}
