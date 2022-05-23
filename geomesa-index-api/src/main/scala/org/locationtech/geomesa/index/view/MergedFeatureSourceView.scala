/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.view

import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.data._
import org.geotools.api.feature.`type`.Name
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.geotools.api.filter.sort.SortBy
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureCollection.GeoMesaFeatureVisitingCollection
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureSource.DelegatingResourceInfo
import org.locationtech.geomesa.index.view.MergedFeatureSourceView.MergedQueryCapabilities
import org.locationtech.geomesa.utils.concurrent.CachedThreadPool

import java.awt.RenderingHints.Key
import java.util.Collections
import java.util.concurrent.CopyOnWriteArrayList

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

  import scala.collection.JavaConverters._

  lazy private val hints = Collections.unmodifiableSet(Collections.emptySet[Key])

  lazy private val capabilities = new MergedQueryCapabilities(sources.map(_._1.getQueryCapabilities))

  override def getSchema: SimpleFeatureType = sft

  override def getCount(query: Query): Int = {
<<<<<<< HEAD
    val total =
      if (parallel) {
        def getSingle(sourceAndFilter: (SimpleFeatureSource, Option[Filter])): Int = {
          val (source, filter) = sourceAndFilter
          source.getCount(mergeFilter(sft, query, filter))
        }
        val results = new CopyOnWriteArrayList[Int]()
        sources.toList.map(s => CachedThreadPool.submit(() => results.add(getSingle(s)))).foreach(_.get)
        results.asScala.foldLeft(0)((sum, count) => if (sum < 0 || count < 0) { -1 } else { sum + count })
      } else {
        // if one of our sources can't get a count (i.e. is negative), give up and return -1
        sources.foldLeft(0) { case (sum, (source, filter)) =>
          lazy val count = source.getCount(mergeFilter(sft, query, filter))
          if (sum < 0 || count < 0) { -1 } else { sum + count }
        }
      }
=======
    // if one of our sources can't get a count (i.e. is negative), give up and return -1
    val total = sources.foldLeft(0) { case (sum, (source, filter)) =>
      lazy val count = source.getCount(mergeFilter(sft, query, filter))
      if (sum < 0 || count < 0) { -1 } else { sum + count }
    }
<<<<<<< HEAD
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> eea6a40faa (GEOMESA-3202 Check for disjoint date queries in merged view store)
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

    val sourceBounds = if (parallel) {
      val results = new CopyOnWriteArrayList[ReferencedEnvelope]()
      sources.toList.map(s => CachedThreadPool.submit(() => getSingle(s).foreach(results.add))).foreach(_.get)
      results.asScala
    } else {
      sources.flatMap(getSingle)
    }

    val bounds = new ReferencedEnvelope(org.locationtech.geomesa.utils.geotools.CRS_EPSG_4326)
    sourceBounds.foreach(bounds.expandToInclude)
    bounds
  }

  override def getBounds(query: Query): ReferencedEnvelope = {
    def getSingle(sourceAndFilter: (SimpleFeatureSource, Option[Filter])): Option[ReferencedEnvelope] =
      Option(sourceAndFilter._1.getBounds(mergeFilter(sft, query, sourceAndFilter._2)))

    val sourceBounds = if (parallel) {
      val results = new CopyOnWriteArrayList[ReferencedEnvelope]()
      sources.toList.map(s => CachedThreadPool.submit(() => getSingle(s).foreach(results.add))).foreach(_.get)
      results.asScala
    } else {
      sources.flatMap(getSingle)
    }

    val bounds = new ReferencedEnvelope(org.locationtech.geomesa.utils.geotools.CRS_EPSG_4326)
<<<<<<< HEAD
    sourceBounds.foreach(bounds.expandToInclude)
=======
    sources.foreach {
      case (source, filter) =>
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
        val source_bounds = source.getBounds(mergeFilter(sft, query, filter))
=======
        val source_bounds = source.getBounds(mergeFilter(query, filter))
>>>>>>> 22da407b4 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
        val source_bounds = source.getBounds(mergeFilter(query, filter))
>>>>>>> 0ab344f339 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
        val source_bounds = source.getBounds(mergeFilter(sft, query, filter))
>>>>>>> eea6a40faa (GEOMESA-3202 Check for disjoint date queries in merged view store)
        if(source_bounds != null){
          bounds.expandToInclude(source_bounds)
        }
    }
<<<<<<< HEAD
>>>>>>> 22da407b47 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 22da407b4 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> 0ab344f339 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
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

    private lazy val featureReader = ds.getFeatureReader(sft, Transaction.AUTO_COMMIT, query)

    override def getSchema: SimpleFeatureType = featureReader.schema

    override def reader(): FeatureReader[SimpleFeatureType, SimpleFeature] = featureReader.reader()

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
    override def supportsSorting(sortAttributes: SortBy*): Boolean =
      capabilities.forall(_.supportsSorting(sortAttributes: _*))
    override def isReliableFIDSupported: Boolean = capabilities.forall(_.isReliableFIDSupported)
    override def isUseProvidedFIDSupported: Boolean = capabilities.forall(_.isUseProvidedFIDSupported)
    override def isJoiningSupported: Boolean = capabilities.forall(_.isJoiningSupported)
    override def isVersionSupported: Boolean = capabilities.forall(_.isVersionSupported)
  }
}
