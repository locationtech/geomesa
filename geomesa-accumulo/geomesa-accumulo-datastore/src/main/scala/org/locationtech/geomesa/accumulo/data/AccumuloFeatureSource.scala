/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.data

import java.util.concurrent.atomic.AtomicBoolean

import com.google.common.cache.{CacheBuilder, CacheLoader}
import com.typesafe.scalalogging.slf4j.Logging
import org.geotools.data._
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureIterator}
import org.geotools.data.store.DataFeatureCollection
import org.geotools.feature.collection.SortedSimpleFeatureCollection
import org.geotools.feature.visitor.{BoundsVisitor, MaxVisitor, MinVisitor}
import org.locationtech.geomesa.accumulo.GeomesaSystemProperties
import org.locationtech.geomesa.accumulo.index.QueryHints.{RichHints, _}
import org.locationtech.geomesa.accumulo.index.QueryPlanner
import org.locationtech.geomesa.accumulo.process.knn.KNNVisitor
import org.locationtech.geomesa.accumulo.process.proximity.ProximityVisitor
import org.locationtech.geomesa.accumulo.process.query.QueryVisitor
import org.locationtech.geomesa.accumulo.process.temporalDensity.TemporalDensityVisitor
import org.locationtech.geomesa.accumulo.process.tube.TubeVisitor
import org.locationtech.geomesa.accumulo.process.unique.AttributeVisitor
import org.locationtech.geomesa.accumulo.util.TryLoggingFailure
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.opengis.feature.FeatureVisitor
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
import org.opengis.filter.expression.{Expression, PropertyName}
import org.opengis.filter.sort.SortBy
import org.opengis.util.ProgressListener

trait AccumuloAbstractFeatureSource extends AbstractFeatureSource with Logging with TryLoggingFailure {
  self =>

  import org.locationtech.geomesa.utils.geotools.Conversions._

  val dataStore: AccumuloDataStore
  val featureName: Name

  def addFeatureListener(listener: FeatureListener) {}

  def removeFeatureListener(listener: FeatureListener) {}

  def getSchema: SimpleFeatureType = getDataStore.getSchema(featureName)

  def getDataStore: AccumuloDataStore = dataStore

  def longCount = dataStore.getRecordTableSize(featureName.getLocalPart)

  // The default behavior for getCount is to use Accumulo to look up the number of entries in
  //  the record table for a feature.
  //  This approach gives a rough upper count for the size of the query results.
  //  For Filter.INCLUDE, this is likely pretty close; all others, it is a lie.

  // Since users may want *actual* counts, there are two ways to force exact counts.
  //  First, one can set the System property "geomesa.force.count".
  //  Second, there is an EXACT_COUNT query hint.
  override def getCount(query: Query) = {
    val exactCount = if(query.getHints.get(EXACT_COUNT) != null) {
      query.getHints.get(EXACT_COUNT).asInstanceOf[Boolean]
    } else {
      GeomesaSystemProperties.QueryProperties.QUERY_EXACT_COUNT.get.toBoolean
    }

    if (exactCount || longCount == -1) {
      getFeaturesNoCache(query).features().size
    } else {
      longCount match {
        case _ if longCount > Int.MaxValue      => Int.MaxValue
        case _                                  => longCount.toInt
      }
    }
  }

  override def getQueryCapabilities = new QueryCapabilities() {
    override def isOffsetSupported = false
    override def isReliableFIDSupported = true
    override def isUseProvidedFIDSupported = true
    override def supportsSorting(sortAttributes: Array[SortBy]) = true
  }

  protected def getFeaturesNoCache(query: Query): SimpleFeatureCollection =
    new AccumuloFeatureCollection(self, query)

  override def getFeatures(query: Query): SimpleFeatureCollection =
    tryLoggingFailures(getFeaturesNoCache(query))

  override def getFeatures(filter: Filter): SimpleFeatureCollection =
    getFeatures(new Query(getSchema().getTypeName, filter))
}

class AccumuloFeatureSource(val dataStore: AccumuloDataStore, val featureName: Name)
  extends AccumuloAbstractFeatureSource

/**
 * Feature collection implementation
 */
class AccumuloFeatureCollection(source: AccumuloAbstractFeatureSource, query: Query)
  extends DataFeatureCollection {

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
      // TODO GEOMESA-421 implement min/max iterators
      case v: MinVisitor if isTime(v.getExpression) =>
        v.setValue(ds.getTimeBounds(query.getTypeName).getStart.toDate)
      case v: MaxVisitor if isTime(v.getExpression) =>
        v.setValue(ds.getTimeBounds(query.getTypeName).getEnd.toDate)

      case v: BoundsVisitor          => v.reset(ds.getBounds(query))
      case v: TubeVisitor            => v.setValue(v.tubeSelect(source, query))
      case v: ProximityVisitor       => v.setValue(v.proximitySearch(source, query))
      case v: QueryVisitor           => v.setValue(v.query(source, query))
      case v: TemporalDensityVisitor => v.setValue(v.query(source, query))
      case v: KNNVisitor             => v.setValue(v.kNNSearch(source,query))
      case v: AttributeVisitor       => v.setValue(v.unique(source, query))
      case _                         => super.accepts(visitor, progress)
    }

  private def isTime(e: Expression) = e match {
    case p: PropertyName => getSchema.getDtgField.exists(_ == p.getPropertyName)
    case _ => false
  }

  override def reader(): FeatureReader[SimpleFeatureType, SimpleFeature] = {
    val reader = ds.getFeatureReader(query.getTypeName, query)
    val maxFeatures = query.getMaxFeatures
    if (maxFeatures != Integer.MAX_VALUE) {
      new MaxFeatureReader[SimpleFeatureType, SimpleFeature](reader, maxFeatures)
    } else {
      reader
    }
  }

  override def getCount = source.getCount(query)

  override def getBounds = source.getBounds(query)
}

class CachingAccumuloFeatureCollection(source: AccumuloAbstractFeatureSource, query: Query)
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

trait CachingFeatureSource extends AccumuloAbstractFeatureSource {
  self: AccumuloAbstractFeatureSource =>

  private val featureCache =
    CacheBuilder.newBuilder().build(
      new CacheLoader[Query, SimpleFeatureCollection] {
        override def load(query: Query): SimpleFeatureCollection =
          new CachingAccumuloFeatureCollection(self, query)
      })

  override def getFeatures(query: Query): SimpleFeatureCollection = {
    // geotools bug in Query.hashCode
    if (query.getStartIndex == null) {
      query.setStartIndex(0)
    }

    if (query.getSortBy == null)
      featureCache.get(query)
    else // Uses mergesort
      new SortedSimpleFeatureCollection(featureCache.get(query), query.getSortBy)
  }

  override def getCount(query: Query): Int = getFeatures(query).size()
}
