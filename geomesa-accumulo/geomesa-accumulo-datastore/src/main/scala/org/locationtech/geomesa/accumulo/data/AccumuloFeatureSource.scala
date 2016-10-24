/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.data

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
import org.locationtech.geomesa.accumulo.GeomesaSystemProperties
import org.locationtech.geomesa.accumulo.index.QueryHints.RichHints
import org.locationtech.geomesa.accumulo.index.QueryPlanner
import org.locationtech.geomesa.accumulo.process.SamplingVisitor
import org.locationtech.geomesa.accumulo.process.knn.KNNVisitor
import org.locationtech.geomesa.accumulo.process.proximity.ProximityVisitor
import org.locationtech.geomesa.accumulo.process.query.QueryVisitor
import org.locationtech.geomesa.accumulo.process.stats.StatsVisitor
import org.locationtech.geomesa.accumulo.process.tube.TubeVisitor
import org.locationtech.geomesa.accumulo.process.unique.AttributeVisitor
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

abstract class AccumuloFeatureSource(dataStore: AccumuloDataStore, override val getSchema: SimpleFeatureType)
    extends SimpleFeatureSource with LazyLogging {

  protected val typeName = getSchema.getTypeName

  lazy private val hints = Collections.unmodifiableSet(Set.empty[Key])

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
    import GeomesaSystemProperties.QueryProperties.QUERY_EXACT_COUNT

    val useExactCount = query.getHints.isExactCount.getOrElse(QUERY_EXACT_COUNT.get.toBoolean)
    lazy val exactCount = dataStore.stats.getCount(getSchema, query.getFilter, exact = true).getOrElse(-1L)

    val count = if (useExactCount) { exactCount } else {
      dataStore.stats.getCount(getSchema, query.getFilter, exact = false).getOrElse(exactCount)
    }
    if (count > Int.MaxValue) Int.MaxValue else count.toInt
  }

  override def getBounds: ReferencedEnvelope = getBounds(new Query(typeName, Filter.INCLUDE))

  override def getBounds(query: Query): ReferencedEnvelope =
    dataStore.stats.getBounds(getSchema, query.getFilter)

  override def getQueryCapabilities = AccumuloQueryCapabilities

  override def getFeatures(query: Query): SimpleFeatureCollection = getFeaturesNoCache(query)

  override def getFeatures(filter: Filter): SimpleFeatureCollection =
    getFeatures(new Query(typeName, filter))

  override def getFeatures: SimpleFeatureCollection = getFeatures(Filter.INCLUDE)

  override def getName: Name = getSchema.getName

  override def getDataStore: AccumuloDataStore = dataStore

  override def getSupportedHints: java.util.Set[Key] = hints

  override def getInfo: ResourceInfo = new DelegatingResourceInfo(this)

  override def addFeatureListener(listener: FeatureListener): Unit = {}

  override def removeFeatureListener(listener: FeatureListener): Unit = {}

  private def getFeaturesNoCache(query: Query): SimpleFeatureCollection =
    new AccumuloFeatureCollection(this, query)

  object AccumuloQueryCapabilities extends QueryCapabilities {
    override def isOffsetSupported = false
    override def isReliableFIDSupported = true
    override def isUseProvidedFIDSupported = true
    override def supportsSorting(sortAttributes: Array[SortBy]) = true
  }
}

/**
 * Feature collection implementation
 */
class AccumuloFeatureCollection(source: AccumuloFeatureSource, query: Query)
  extends DataFeatureCollection(AccumuloFeatureCollection.nextId) {

  private val ds = source.getDataStore
  private val open = new AtomicBoolean(false)

  override def getSchema: SimpleFeatureType = {
    if (!open.get()) {
      // once opened the query will already be configured by the query planner,
      // otherwise we have to compute it here
      QueryPlanner.configureQuery(query, source.getSchema)
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
      case v: BoundsVisitor    => v.reset(ds.stats.getBounds(source.getSchema, query.getFilter))
      case v: TubeVisitor      => v.setValue(v.tubeSelect(source, query))
      case v: ProximityVisitor => v.setValue(v.proximitySearch(source, query))
      case v: QueryVisitor     => v.setValue(v.query(source, query))
      case v: StatsVisitor     => v.setValue(v.query(source, query))
      case v: SamplingVisitor  => v.setValue(v.sample(source, query))
      case v: KNNVisitor       => v.setValue(v.kNNSearch(source,query))
      case v: AttributeVisitor => v.setValue(v.unique(source, query))

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

  private def minMax(attribute: String, exact: Boolean): Option[(Any, Any)] =
    ds.stats.getAttributeBounds[Any](source.getSchema, attribute, query.getFilter, exact).map(b => (b.lower, b.upper))

  override def reader(): FeatureReader[SimpleFeatureType, SimpleFeature] =
    ds.getFeatureReader(query, Transaction.AUTO_COMMIT)

  override def getCount = source.getCount(query)

  override def getBounds = source.getBounds(query)
}

object AccumuloFeatureCollection {
  private val oneUp = new AtomicLong(0)
  def nextId: String = s"AccumuloFeatureCollection-${oneUp.getAndIncrement()}"
}

class CachingAccumuloFeatureCollection(source: AccumuloFeatureSource, query: Query)
    extends AccumuloFeatureCollection(source, query) {

  lazy val featureList = {
    // use ListBuffer for constant append time and size
    val buf = scala.collection.mutable.ListBuffer.empty[SimpleFeature]
    val iter = super.features

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
}

trait CachingFeatureSource extends AccumuloFeatureSource {

  private val featureCache =
    Caffeine.newBuilder().build(
      new CacheLoader[Query, SimpleFeatureCollection] {
        override def load(query: Query): SimpleFeatureCollection =
          new CachingAccumuloFeatureCollection(CachingFeatureSource.this, query)
      })

  abstract override def getFeatures(query: Query): SimpleFeatureCollection = {
    // geotools bug in Query.hashCode
    if (query.getStartIndex == null) {
      query.setStartIndex(0)
    }

    if (query.getSortBy == null)
      featureCache.get(query)
    else // Uses mergesort
      new SortedSimpleFeatureCollection(featureCache.get(query), query.getSortBy)
  }

  abstract override def getCount(query: Query): Int = getFeatures(query).size()
}

class DelegatingResourceInfo(source: SimpleFeatureSource) extends ResourceInfo {

  import scala.collection.JavaConversions._

  private val keywords = Collections.unmodifiableSet(Set("features", getName) ++ source.getSchema.getKeywords)

  override def getName: String = source.getSchema.getTypeName

  override def getTitle: String = source.getSchema.getName.getLocalPart

  override def getDescription: String = null

  override def getKeywords: util.Set[String] = keywords

  override def getSchema: URI = Try(new URI(source.getSchema.getName.getNamespaceURI)).getOrElse(null)

  override def getCRS: CoordinateReferenceSystem = source.getSchema.getCoordinateReferenceSystem

  override def getBounds: ReferencedEnvelope = source.getBounds
}
