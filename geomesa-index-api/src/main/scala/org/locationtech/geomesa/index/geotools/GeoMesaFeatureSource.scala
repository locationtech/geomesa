/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.geotools

import java.awt.RenderingHints.Key
import java.net.URI
import java.util
import java.util.Collections

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import com.typesafe.scalalogging.LazyLogging
import org.geotools.data._
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureIterator, SimpleFeatureSource}
import org.geotools.feature.collection.SortedSimpleFeatureCollection
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureSource.{DelegatingResourceInfo, GeoMesaQueryCapabilities}
import org.locationtech.geomesa.index.planning.QueryRunner
import org.locationtech.geomesa.index.stats.HasGeoMesaStats
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.opengis.feature.FeatureVisitor
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
import org.opengis.filter.sort.SortBy
import org.opengis.referencing.crs.CoordinateReferenceSystem
import org.opengis.util.ProgressListener

import scala.collection.JavaConversions._
import scala.util.Try

class GeoMesaFeatureSource(val ds: DataStore with HasGeoMesaStats,
                           val sft: SimpleFeatureType,
                           private [geotools] val runner: QueryRunner)
    extends SimpleFeatureSource with LazyLogging {

  lazy private val hints = Collections.unmodifiableSet(Collections.emptySet[Key])

  override def getSchema: SimpleFeatureType = sft

  /**
    * The default behavior for getCount is to use estimated statistics if available, or -1 to indicate
    * that the operation would be expensive (@see org.geotools.data.FeatureSource#getCount(org.geotools.data.Query)).
    *
    * Since users may want <b>exact</b> counts, there are two ways to force exact counts:
    *   1. use the system property "geomesa.force.count"
    *   2. use the EXACT_COUNT query hint
    *
    * @param query query
    * @return
    */
  override def getCount(query: Query): Int = {
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints
    import org.locationtech.geomesa.index.conf.QueryProperties.QueryExactCount

    val useExactCount = query.getHints.isExactCount.getOrElse(QueryExactCount.get.toBoolean)
    val count = ds.stats.getCount(getSchema, query.getFilter, useExactCount).getOrElse(-1L)

    if (count > Int.MaxValue) {
      logger.warn(s"Truncating count $count to Int.MaxValue (${Int.MaxValue})")
      Int.MaxValue
    } else {
      count.toInt
    }
  }

  override def getBounds: ReferencedEnvelope = getBounds(new Query(sft.getTypeName, Filter.INCLUDE))

  override def getBounds(query: Query): ReferencedEnvelope =
    ds.stats.getBounds(getSchema, query.getFilter)

  override def getQueryCapabilities: QueryCapabilities = GeoMesaQueryCapabilities

  override def getFeatures: SimpleFeatureCollection = getFeatures(Filter.INCLUDE)

  override def getFeatures(filter: Filter): SimpleFeatureCollection =
    getFeatures(new Query(sft.getTypeName, filter))

  override def getFeatures(original: Query): SimpleFeatureCollection = {
    val query = if (original.getTypeName != null) { original } else {
      logger.debug(s"Received Query with null typeName, setting to: ${sft.getTypeName}")
      val nq = new Query(original)
      nq.setTypeName(sft.getTypeName)
      nq
    }
    new GeoMesaFeatureCollection(this, query)
  }

  override def getName: Name = getSchema.getName

  override def getDataStore: DataStore = ds

  override def getSupportedHints: java.util.Set[Key] = hints

  override def getInfo: ResourceInfo = new DelegatingResourceInfo(this)

  override def addFeatureListener(listener: FeatureListener): Unit = throw new NotImplementedError()

  override def removeFeatureListener(listener: FeatureListener): Unit = throw new NotImplementedError()
}

object GeoMesaFeatureSource {

  object GeoMesaQueryCapabilities extends QueryCapabilities {
    override def isOffsetSupported = false
    override def isReliableFIDSupported = true
    override def isUseProvidedFIDSupported = true
    override def supportsSorting(sortAttributes: Array[SortBy]) = true
  }

  trait CachingFeatureSource extends GeoMesaFeatureSource {

    private val featureCache =
      Caffeine.newBuilder().build(
        new CacheLoader[Query, SimpleFeatureCollection] {
          override def load(query: Query): SimpleFeatureCollection =
            new CachingFeatureCollection(new GeoMesaFeatureCollection(CachingFeatureSource.this, query))
        })

    abstract override def getFeatures(query: Query): SimpleFeatureCollection = {
      // geotools bug in Query.hashCode
      if (query.getStartIndex == null) {
        query.setStartIndex(0)
      }

      if (query.getSortBy == null) {
        featureCache.get(query)
      } else {
        // Uses mergesort
        new SortedSimpleFeatureCollection(featureCache.get(query), query.getSortBy)
      }
    }

    abstract override def getCount(query: Query): Int = getFeatures(query).size()
  }

  /*_*/
  class CachingFeatureCollection(delegate: SimpleFeatureCollection) extends SimpleFeatureCollection {
  /*_*/

    lazy private val featureList = {
      // use ListBuffer for constant append time and size
      val buf = scala.collection.mutable.ListBuffer.empty[SimpleFeature]
      val iter = delegate.features

      while (iter.hasNext) {
        buf.append(iter.next())
      }
      iter.close()
      buf
    }

    override def features: SimpleFeatureIterator = new SimpleFeatureIterator() {
      private val iter = featureList.iterator
      override def hasNext: Boolean = iter.hasNext
      override def next: SimpleFeature = iter.next()
      override def close(): Unit = {}
    }

    override def size: Int = featureList.length

    override def toArray: Array[AnyRef] = featureList.toArray

    /*_*/
    override def toArray[O](a: Array[O with Object]): Array[O with Object] = {
      import scala.collection.JavaConverters._
      // noinspection ScalaRedundantCast
      featureList.asJava.toArray(a).asInstanceOf[Array[O with Object]]
    }
    /*_*/

    override def contains(o: scala.Any): Boolean = featureList.contains(o)

    override def containsAll(o: util.Collection[_]): Boolean = featureList.containsAll(o)

    override def isEmpty: Boolean = featureList.isEmpty

    override def subCollection(filter: Filter): SimpleFeatureCollection =
      new CachingFeatureCollection(delegate.subCollection(filter))

    override def sort(order: SortBy): SimpleFeatureCollection = delegate.sort(order)

    override def getID: String = delegate.getID

    override def accepts(visitor: FeatureVisitor, progress: ProgressListener): Unit =
      delegate.accepts(visitor, progress)

    override def getSchema: SimpleFeatureType = delegate.getSchema

    override def getBounds: ReferencedEnvelope = delegate.getBounds
  }

  class DelegatingResourceInfo(source: SimpleFeatureSource) extends ResourceInfo {

    import scala.collection.JavaConversions._

    private val keywords = Collections.unmodifiableSet(Set("features", getName) ++ source.getSchema.getKeywords)

    override def getName: String = source.getSchema.getName.getURI

    override def getTitle: String = source.getSchema.getName.getLocalPart

    override def getDescription: String = null

    override def getKeywords: util.Set[String] = keywords

    override def getSchema: URI = Try(new URI(source.getSchema.getName.getNamespaceURI)).getOrElse(null)

    override def getCRS: CoordinateReferenceSystem = source.getSchema.getCoordinateReferenceSystem

    override def getBounds: ReferencedEnvelope = source.getBounds
  }
}
