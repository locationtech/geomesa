/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
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
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import com.typesafe.scalalogging.LazyLogging
import org.geotools.data._
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureIterator, SimpleFeatureSource}
import org.geotools.data.store.DataFeatureCollection
import org.geotools.feature.collection.SortedSimpleFeatureCollection
import org.geotools.feature.visitor.{BoundsVisitor, MaxVisitor, MinVisitor}
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.opengis.feature.FeatureVisitor
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
import org.opengis.filter.expression.PropertyName
import org.opengis.filter.sort.SortBy
import org.opengis.referencing.crs.CoordinateReferenceSystem
import org.opengis.util.ProgressListener

import scala.collection.JavaConversions._
import scala.util.Try

class GeoMesaFeatureSource(val ds: GeoMesaDataStore[_, _, _],
                           val sft: SimpleFeatureType,
                           protected val collection: (Query, GeoMesaFeatureSource) => GeoMesaFeatureCollection)
    extends SimpleFeatureSource with LazyLogging {

  lazy private val hints = Collections.unmodifiableSet(Set.empty[Key])

  override def getSchema: SimpleFeatureType = sft

  /**
   * The default behavior for getCount is to use estimated statistics.
   * In most cases, this should be fairly close.
   *
   * Since users may want <b>exact</b> counts, there are two ways to force exact counts.
   * First, one can set the System property "geomesa.force.count".
   * Second, there is an EXACT_COUNT query hint.
   *
   * @param query query
   * @return count
   */
  override def getCount(query: Query): Int = {
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints
    import org.locationtech.geomesa.index.conf.QueryProperties.QUERY_EXACT_COUNT

    val useExactCount = query.getHints.isExactCount.getOrElse(QUERY_EXACT_COUNT.get.toBoolean)
    lazy val exactCount = ds.stats.getCount(getSchema, query.getFilter, exact = true).getOrElse(-1L)

    val count = if (useExactCount) { exactCount } else {
      ds.stats.getCount(getSchema, query.getFilter, exact = false).getOrElse(exactCount)
    }
    if (count > Int.MaxValue) Int.MaxValue else count.toInt
  }

  override def getBounds: ReferencedEnvelope = getBounds(new Query(sft.getTypeName, Filter.INCLUDE))

  override def getBounds(query: Query): ReferencedEnvelope =
    ds.stats.getBounds(getSchema, query.getFilter)

  override def getQueryCapabilities = GeoMesaQueryCapabilities

  override def getFeatures: SimpleFeatureCollection = getFeatures(Filter.INCLUDE)

  override def getFeatures(filter: Filter): SimpleFeatureCollection =
    getFeatures(new Query(sft.getTypeName, filter))

  override def getFeatures(query: Query): SimpleFeatureCollection = collection(query, this)

  override def getName: Name = getSchema.getName

  override def getDataStore: DataStore = ds

  override def getSupportedHints: java.util.Set[Key] = hints

  override def getInfo: ResourceInfo = new DelegatingResourceInfo(this)

  override def addFeatureListener(listener: FeatureListener): Unit = throw new NotImplementedError()

  override def removeFeatureListener(listener: FeatureListener): Unit = throw new NotImplementedError()

  object GeoMesaQueryCapabilities extends QueryCapabilities {
    override def isOffsetSupported = false
    override def isReliableFIDSupported = true
    override def isUseProvidedFIDSupported = true
    override def supportsSorting(sortAttributes: Array[SortBy]) = true
  }
}

/**
 * Feature collection implementation
 */
class GeoMesaFeatureCollection(private [geotools] val source: GeoMesaFeatureSource, private [geotools] val query: Query)
  extends DataFeatureCollection(GeoMesaFeatureCollection.nextId) {

  private val open = new AtomicBoolean(false)

  override def getSchema: SimpleFeatureType = {
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints
    if (!open.get()) {
      // once opened the query will already be configured by the query planner,
      // otherwise we have to compute it here
      source.ds.queryPlanner.configureQuery(query, source.getSchema)
    }
    query.getHints.getReturnSft
  }

  override def openIterator(): java.util.Iterator[SimpleFeature] = {
    val iter = super.openIterator()
    open.set(true)
    iter
  }

  override def accepts(visitor: FeatureVisitor, progress: ProgressListener): Unit =
    visitor match {
      case v: BoundsVisitor    => v.reset(source.ds.stats.getBounds(source.getSchema, query.getFilter))

      case v: MinVisitor if v.getExpression.isInstanceOf[PropertyName] =>
        val attribute = v.getExpression.asInstanceOf[PropertyName].getPropertyName
        minMax(attribute, exact = false).orElse(minMax(attribute, exact = true)) match {
          case Some((min, max)) => v.setValue(min)
          case None             => super.accepts(visitor, progress)
        }

      case v: MaxVisitor if v.getExpression.isInstanceOf[PropertyName] =>
        val attribute = v.getExpression.asInstanceOf[PropertyName].getPropertyName
        minMax(attribute, exact = false).orElse(minMax(attribute, exact = true)) match {
          case Some((min, max)) => v.setValue(max)
          case None             => super.accepts(visitor, progress)
        }

      case _ => super.accepts(visitor, progress)
    }

  private def minMax(attribute: String, exact: Boolean): Option[(Any, Any)] = {
    val bounds = source.ds.stats.getAttributeBounds[Any](source.getSchema, attribute, query.getFilter, exact)
    bounds.map(b => (b.lower, b.upper))
  }

  override def reader(): FeatureReader[SimpleFeatureType, SimpleFeature] =
    source.ds.getFeatureReader(query, Transaction.AUTO_COMMIT)

  override def getBounds: ReferencedEnvelope = source.getBounds(query)

  override def getCount: Int = source.getCount(query)

  override def size: Int = getCount
}

object GeoMesaFeatureCollection {
  private val oneUp = new AtomicLong(0)
  def nextId: String = s"GeoMesaFeatureCollection-${oneUp.getAndIncrement()}"
}

trait CachingFeatureSource extends GeoMesaFeatureSource {

  private val featureCache =
    Caffeine.newBuilder().build(
      new CacheLoader[Query, SimpleFeatureCollection] {
        override def load(query: Query): SimpleFeatureCollection =
          new CachingFeatureCollection(collection(query, CachingFeatureSource.this))
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

  lazy val featureList = {
    // use ListBuffer for constant append time and size
    val buf = scala.collection.mutable.ListBuffer.empty[SimpleFeature]
    val iter = delegate.features

    while (iter.hasNext) {
      buf.append(iter.next())
    }
    iter.close()
    buf
  }

  override def features = new SimpleFeatureIterator() {
    private val iter = featureList.iterator
    override def hasNext = iter.hasNext
    override def next = iter.next()
    override def close() = {}
  }

  override def size = featureList.length

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
