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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> b71311c31d (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7933021402 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> e22e621f59 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
>>>>>>> bdd2bd6424 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 985fbd05df (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 6f8af866fb (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 7933021402 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
    // if one of our sources can't get a count (i.e. is negative), give up and return -1
    val total = sources.foldLeft(0) { case (sum, (source, filter)) =>
      lazy val count = source.getCount(mergeFilter(sft, query, filter))
      if (sum < 0 || count < 0) { -1 } else { sum + count }
    }
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e82ce16cce (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> eea6a40faa (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> b71311c31d (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 7933021402 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> e22e621f59 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> bdd2bd6424 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 985fbd05df (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 6f8af866fb (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> e82ce16cce (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
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
    sourceBounds.foreach(bounds.expandToInclude)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 39a3effa59 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5db8123db2 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4c216bcec1 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b78e3b07c2 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f21a90a3ba (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 4c216bcec1 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 5db8123db2 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
    sources.foreach {
      case (source, filter) =>
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 99963d4974 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 2c2f1db1dc (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 380e104c6d (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 7184aa0939 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 87215515d4 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> d4916a10b5 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 94734e95ce (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> ed446c4a76 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> bebf4c3e7d (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> a24ee8e060 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> c4103ae4e3 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 4697868530 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 0c2854c936 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 7b23a6a2e6 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 49093c2fe1 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 17b5ca6708 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> b3286af625 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b114c31d7b (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> d4f1ac3977 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 0a65fe97ef (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> bc92b610a0 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7933021402 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> cf889fdf30 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7221b07f1c (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> aa95961d51 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9e2a070ec3 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 3cfe113c76 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 0733de3db3 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 78ced06e27 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> c0ff3527a9 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 49093c2fe1 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 0a65fe97e (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> d017c8b7bf (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
<<<<<<< HEAD
>>>>>>> bc92b610a (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 35b8e2b607 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
>>>>>>> c4103ae4e3 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 7933021402 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 4697868530 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 7221b07f1c (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 0c2854c936 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 7b23a6a2e6 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 49093c2fe1 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> 17b5ca670 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
>>>>>>> dbcb800c14 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> b6e4df392e (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> d2549e87c7 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 19eba2a6c8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 17b5ca670 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> e6cd678a41 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> b3286af62 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
>>>>>>> b5f42c9078 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
>>>>>>> b114c31d7 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
>>>>>>> cf889fdf30 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
>>>>>>> d4f1ac397 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7b23a6a2e6 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> 7221b07f1c (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
=======
>>>>>>> 17b5ca670 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> dbcb800c1 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa95961d51 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
>>>>>>> b6e4df392 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
>>>>>>> 9e2a070ec3 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
>>>>>>> d2549e87c (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
>>>>>>> 3cfe113c76 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
>>>>>>> 19eba2a6c (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 0733de3db3 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
=======
>>>>>>> 17b5ca670 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> e6cd678a4 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> 78ced06e27 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> f322323fbe (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 7ef627da1e (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 81fe057376 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 17b5ca670 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> 6c414ca5c5 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> ddf8b5262d (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> dbf6f4d9b4 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> e82ce16cce (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 17b5ca6708 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> 374ed605ee (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 2c2f1db1dc (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
>>>>>>> b114c31d7b (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> 380e104c6d (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 7184aa0939 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
=======
>>>>>>> 0a65fe97ef (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 17b5ca670 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> dbcb800c14 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
>>>>>>> 87215515d4 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
>>>>>>> b6e4df392e (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
>>>>>>> 94734e95ce (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
>>>>>>> d2549e87c7 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
>>>>>>> ed446c4a76 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
>>>>>>> 19eba2a6c8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> bebf4c3e7d (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> a24ee8e060 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
=======
>>>>>>> b3286af62 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
>>>>>>> b5f42c9078 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
>>>>>>> 0686ae15da (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
=======
>>>>>>> b114c31d7 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> cf889fdf30 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> 4697868530 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 7221b07f1c (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 0c2854c936 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> aa95961d51 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> 7b23a6a2e6 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
        val source_bounds = source.getBounds(mergeFilter(sft, query, filter))
=======
        val source_bounds = source.getBounds(mergeFilter(query, filter))
>>>>>>> 22da407b4 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 374ed605ee (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 2c2f1db1dc (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 380e104c6d (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 87215515d4 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 94734e95ce (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> ed446c4a76 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> a24ee8e060 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 0686ae15da (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 4697868530 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 7b23a6a2e6 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
        val source_bounds = source.getBounds(mergeFilter(query, filter))
>>>>>>> 0ab344f339 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
        val source_bounds = source.getBounds(mergeFilter(sft, query, filter))
>>>>>>> eea6a40faa (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 17b5ca6708 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
        val source_bounds = source.getBounds(mergeFilter(query, filter))
>>>>>>> 74d905136b (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f21a90a3ba (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
    sources.foreach {
      case (source, filter) =>
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
        val source_bounds = source.getBounds(mergeFilter(query, filter))
>>>>>>> 6a4564f895 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
        val source_bounds = source.getBounds(mergeFilter(sft, query, filter))
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
=======
>>>>>>> b3286af625 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
>>>>>>> dbcb800c14 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e6cd678a41 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> b5f42c9078 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> cf889fdf30 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
<<<<<<< HEAD
>>>>>>> 9e2a070ec3 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 3cfe113c76 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 78ced06e27 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> dbcb800c1 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> aa95961d51 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> e6cd678a4 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 6c414ca5c5 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
>>>>>>> 87215515d4 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> e6cd678a41 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> a24ee8e060 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 0686ae15da (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> cf889fdf30 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 4697868530 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> dbcb800c1 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> aa95961d51 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 7b23a6a2e6 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
        val source_bounds = source.getBounds(mergeFilter(query, filter))
>>>>>>> 0ab344f33 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> bebf4c3e7d (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> b114c31d7b (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
        val source_bounds = source.getBounds(mergeFilter(sft, query, filter))
>>>>>>> eea6a40fa (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a24ee8e060 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 0c2854c936 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 7b23a6a2e6 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> d4f1ac3977 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 7221b07f1c (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa95961d51 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 0733de3db3 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 78ced06e27 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
>>>>>>> 0c2854c936 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> aa95961d51 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 7b23a6a2e6 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> 17b5ca670 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c4103ae4e3 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> dbcb800c14 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
        val source_bounds = source.getBounds(mergeFilter(query, filter))
>>>>>>> 74d905136 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 49093c2fe1 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> 0a65fe97ef (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
    sources.foreach {
      case (source, filter) =>
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
        val source_bounds = source.getBounds(mergeFilter(query, filter))
>>>>>>> b6daad9ec3 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
        val source_bounds = source.getBounds(mergeFilter(sft, query, filter))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b71311c31d (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> b6e4df392e (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ed446c4a76 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
        val source_bounds = source.getBounds(mergeFilter(query, filter))
>>>>>>> 0ab344f33 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> d2549e87c7 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> bebf4c3e7d (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
        val source_bounds = source.getBounds(mergeFilter(sft, query, filter))
>>>>>>> eea6a40fa (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 19eba2a6c8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a24ee8e060 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
>>>>>>> 17b5ca670 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> e6cd678a41 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c4103ae4e3 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
        val source_bounds = source.getBounds(mergeFilter(query, filter))
>>>>>>> 74d905136 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> bc92b610a0 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> d017c8b7bf (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
<<<<<<< HEAD
>>>>>>> c0ff3527a9 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
>>>>>>> 49093c2fe1 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
        val source_bounds = source.getBounds(mergeFilter(sft, query, filter))
>>>>>>> 2fc500c49 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 7933021402 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
=======
    sources.foreach {
      case (source, filter) =>
<<<<<<< HEAD
<<<<<<< HEAD
        val source_bounds = source.getBounds(mergeFilter(query, filter))
>>>>>>> 6a4564f89 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
>>>>>>> 234114a499 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
>>>>>>> 4c216bcec1 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
=======
        val source_bounds = source.getBounds(mergeFilter(sft, query, filter))
>>>>>>> 051bc58bc (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 63a7a37cdc (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> e22e621f59 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
=======
>>>>>>> b3286af62 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ac3a703269 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
>>>>>>> b5f42c9078 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
=======
=======
        val source_bounds = source.getBounds(mergeFilter(query, filter))
>>>>>>> 0ab344f33 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> b114c31d7 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 369ec0ce2b (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
>>>>>>> cf889fdf30 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
=======
>>>>>>> d4f1ac397 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> be2554eb71 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 7221b07f1c (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
=======
=======
>>>>>>> 17b5ca670 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> dbcb800c1 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 350ba6beb5 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
>>>>>>> aa95961d51 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
=======
>>>>>>> 0a65fe97e (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
>>>>>>> 1ba46ef3b6 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
>>>>>>> d017c8b7bf (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
=======
=======
    sources.foreach {
      case (source, filter) =>
<<<<<<< HEAD
        val source_bounds = source.getBounds(mergeFilter(query, filter))
>>>>>>> b6daad9ec (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
>>>>>>> a7b9fb6032 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
>>>>>>> b78e3b07c2 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
=======
        val source_bounds = source.getBounds(mergeFilter(sft, query, filter))
>>>>>>> b71311c31 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> ff221938ac (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> bdd2bd6424 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
=======
>>>>>>> b6e4df392 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
>>>>>>> 824cbb85a8 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
>>>>>>> 9e2a070ec3 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
=======
=======
        val source_bounds = source.getBounds(mergeFilter(query, filter))
>>>>>>> 0ab344f33 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> d2549e87c (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
>>>>>>> ac6c032f35 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
>>>>>>> 3cfe113c76 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
=======
>>>>>>> 19eba2a6c (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 6af8d5b1ec (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 0733de3db3 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
=======
=======
>>>>>>> 17b5ca670 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> e6cd678a4 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
>>>>>>> 825c849338 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
>>>>>>> 78ced06e27 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
=======
>>>>>>> bc92b610a (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> 5b800f4662 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> c0ff3527a9 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
    sources.foreach {
      case (source, filter) =>
<<<<<<< HEAD
        val source_bounds = source.getBounds(mergeFilter(query, filter))
>>>>>>> 57b1217f56 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
        val source_bounds = source.getBounds(mergeFilter(sft, query, filter))
>>>>>>> 985fbd05df (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> f322323fbe (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
        val source_bounds = source.getBounds(mergeFilter(query, filter))
>>>>>>> 0ab344f33 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> 7ef627da1e (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
        val source_bounds = source.getBounds(mergeFilter(sft, query, filter))
>>>>>>> eea6a40fa (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 81fe057376 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 17b5ca670 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> 6c414ca5c5 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
        val source_bounds = source.getBounds(mergeFilter(query, filter))
>>>>>>> 74d905136 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> 35b8e2b607 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
    sources.foreach {
      case (source, filter) =>
<<<<<<< HEAD
        val source_bounds = source.getBounds(mergeFilter(query, filter))
>>>>>>> 708d45330c (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
        val source_bounds = source.getBounds(mergeFilter(sft, query, filter))
>>>>>>> 6f8af866fb (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> ddf8b5262d (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
        val source_bounds = source.getBounds(mergeFilter(query, filter))
>>>>>>> 0ab344f339 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> dbf6f4d9b4 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> e82ce16cce (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 17b5ca6708 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> 374ed605ee (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 99963d4974 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
        val source_bounds = source.getBounds(mergeFilter(query, filter))
>>>>>>> 6a4564f895 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> f21a90a3ba (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> b3286af625 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
>>>>>>> 2c2f1db1dc (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
=======
        val source_bounds = source.getBounds(mergeFilter(query, filter))
>>>>>>> 0ab344f33 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
>>>>>>> b114c31d7b (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
>>>>>>> 380e104c6d (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
=======
        val source_bounds = source.getBounds(mergeFilter(sft, query, filter))
>>>>>>> eea6a40fa (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> d4f1ac3977 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 7184aa0939 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> d4f1ac3977 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 17b5ca670 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
>>>>>>> dbcb800c14 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
>>>>>>> 87215515d4 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
=======
        val source_bounds = source.getBounds(mergeFilter(query, filter))
>>>>>>> 74d905136 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> 0a65fe97ef (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> d4916a10b5 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
        val source_bounds = source.getBounds(mergeFilter(query, filter))
>>>>>>> b6daad9ec3 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> 39a3effa59 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> b71311c31d (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 94734e95ce (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> ed446c4a76 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> bebf4c3e7d (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> a24ee8e060 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> c4103ae4e3 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
=======
<<<<<<< HEAD
        val source_bounds = source.getBounds(mergeFilter(sft, query, filter))
>>>>>>> 2fc500c49 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 7933021402 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
=======
=======
    sources.foreach {
      case (source, filter) =>
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
        val source_bounds = source.getBounds(mergeFilter(query, filter))
>>>>>>> 6a4564f89 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
>>>>>>> 234114a499 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
>>>>>>> 4c216bcec1 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
>>>>>>> 5db8123db2 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
=======
=======
        val source_bounds = source.getBounds(mergeFilter(sft, query, filter))
>>>>>>> 051bc58bc (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 0686ae15da (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> 63a7a37cdc (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> e22e621f59 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
=======
=======
>>>>>>> b3286af62 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 4697868530 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> ac3a703269 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
>>>>>>> b5f42c9078 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
>>>>>>> 0686ae15da (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
=======
=======
=======
        val source_bounds = source.getBounds(mergeFilter(query, filter))
>>>>>>> 0ab344f33 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> b114c31d7 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 0c2854c936 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 369ec0ce2b (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
>>>>>>> cf889fdf30 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
>>>>>>> 4697868530 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
=======
=======
>>>>>>> d4f1ac397 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 7b23a6a2e6 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> be2554eb71 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 7221b07f1c (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 0c2854c936 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
=======
=======
=======
>>>>>>> 17b5ca670 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> dbcb800c1 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 49093c2fe1 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> 350ba6beb5 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
>>>>>>> aa95961d51 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
>>>>>>> 7b23a6a2e6 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
=======
=======
>>>>>>> 0a65fe97e (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> 1ba46ef3b6 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> d017c8b7bf (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> 49093c2fe1 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
        if(source_bounds != null){
          bounds.expandToInclude(source_bounds)
        }
    }
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f21a90a3ba (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 39a3effa59 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> 22da407b47 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 22da407b4 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5db8123db2 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
>>>>>>> 0ab344f339 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 22da407b4 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> 74d905136b (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 22da407b4 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> 6a4564f895 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 22da407b4 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> b6daad9ec3 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b78e3b07c2 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 6a4564f89 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
>>>>>>> 4c216bcec1 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> b6daad9ec (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
>>>>>>> b78e3b07c2 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 22da407b4 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> 57b1217f56 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 22da407b47 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> 708d45330c (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
>>>>>>> 99963d4974 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> 22da407b47 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 22da407b4 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> 0ab344f339 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
>>>>>>> dbf6f4d9b4 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
>>>>>>> 22da407b4 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> 74d905136b (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> 99963d4974 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 22da407b4 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> 6a4564f895 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
>>>>>>> f21a90a3ba (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 39a3effa59 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 6a4564f89 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
>>>>>>> 4c216bcec1 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
<<<<<<< HEAD
>>>>>>> 5db8123db2 (GEOMESA-3153 Fix merged view to only expand bounds on non-null bounds (#2814))
=======
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
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
