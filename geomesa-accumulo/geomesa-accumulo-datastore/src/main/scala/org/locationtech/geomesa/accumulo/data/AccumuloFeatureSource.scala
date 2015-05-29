/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.accumulo.data

import com.google.common.cache.{CacheBuilder, CacheLoader}
import com.typesafe.scalalogging.slf4j.Logging
import org.geotools.data._
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureIterator, SimpleFeatureSource}
import org.geotools.feature.collection.SortedSimpleFeatureCollection
import org.geotools.feature.visitor.{BoundsVisitor, MaxVisitor, MinVisitor}
import org.locationtech.geomesa.accumulo.index.QueryHints._
import org.locationtech.geomesa.accumulo.iterators.TemporalDensityIterator.createFeatureType
import org.locationtech.geomesa.accumulo.process.knn.KNNVisitor
import org.locationtech.geomesa.accumulo.process.proximity.ProximityVisitor
import org.locationtech.geomesa.accumulo.process.query.QueryVisitor
import org.locationtech.geomesa.accumulo.process.temporalDensity.TemporalDensityVisitor
import org.locationtech.geomesa.accumulo.process.tube.TubeVisitor
import org.locationtech.geomesa.accumulo.process.unique.AttributeVisitor
import org.locationtech.geomesa.accumulo.util.TryLoggingFailure
import org.opengis.feature.FeatureVisitor
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
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
    val exactCount = query.getHints.get(EXACT_COUNT) == java.lang.Boolean.TRUE ||
                     System.getProperty("geomesa.force.count") == "true"

    if (exactCount || longCount == -1) {
      getFeaturesNoCache(query).features().size
    } else {
      longCount match {
        case _ if longCount > Int.MaxValue      => Int.MaxValue
        case _                                  => longCount.toInt
      }
    }
  }

  override def getQueryCapabilities =
    new QueryCapabilities() {
      override def isOffsetSupported = false
      override def isReliableFIDSupported = true
      override def isUseProvidedFIDSupported = true
      override def supportsSorting(sortAttributes: Array[SortBy]) = true
    }

  protected def getFeaturesNoCache(query: Query): SimpleFeatureCollection = {
    org.locationtech.geomesa.accumulo.index.setQueryTransforms(query, getSchema)
    new AccumuloFeatureCollection(self, query)
  }

  override def getFeatures(query: Query): SimpleFeatureCollection =
    tryLoggingFailures(getFeaturesNoCache(query))

  override def getFeatures(filter: Filter): SimpleFeatureCollection =
    getFeatures(new Query(getSchema().getTypeName, filter))
}

class AccumuloFeatureSource(val dataStore: AccumuloDataStore, val featureName: Name)
  extends AccumuloAbstractFeatureSource

class AccumuloFeatureCollection(source: SimpleFeatureSource, query: Query)
  extends DefaultFeatureResults(source, query) {

  val ds  = source.getDataStore.asInstanceOf[AccumuloDataStore]

  override def getSchema: SimpleFeatureType =
    if (query.getHints.containsKey(TEMPORAL_DENSITY_KEY)) {
      createFeatureType(source.getSchema())
    } else {
      org.locationtech.geomesa.accumulo.index.getTransformSchema(query).getOrElse(super.getSchema)
    }

  override def accepts(visitor: FeatureVisitor, progress: ProgressListener) =
    visitor match {
      // TODO GEOMESA-421 implement min/max iterators
      case v: MinVisitor             => v.setValue(ds.getTimeBounds(query.getTypeName).getStart.toDate)
      case v: MaxVisitor             => v.setValue(ds.getTimeBounds(query.getTypeName).getEnd.toDate)
      case v: BoundsVisitor          => v.reset(ds.getBounds(query))
      case v: TubeVisitor            => v.setValue(v.tubeSelect(source, query))
      case v: ProximityVisitor       => v.setValue(v.proximitySearch(source, query))
      case v: QueryVisitor           => v.setValue(v.query(source, query))
      case v: TemporalDensityVisitor => v.setValue(v.query(source, query))
      case v: KNNVisitor             => v.setValue(v.kNNSearch(source,query))
      case v: AttributeVisitor       => v.setValue(v.unique(source, query))
      case _                         => super.accepts(visitor, progress)
    }

  override def reader(): FeatureReader[SimpleFeatureType, SimpleFeature] = super.reader()
}

class CachingAccumuloFeatureCollection(source: SimpleFeatureSource, query: Query)
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
    override def next = iter.next
    override def close = {}
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
