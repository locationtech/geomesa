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

package org.locationtech.geomesa.core.data

import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.geotools.data._
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureSource}
import org.geotools.feature.visitor.{BoundsVisitor, MaxVisitor, MinVisitor}
import org.joda.time.DateTime
import org.locationtech.geomesa.core.process.knn.KNNVisitor
import org.locationtech.geomesa.core.process.proximity.ProximityVisitor
import org.locationtech.geomesa.core.process.query.QueryVisitor
import org.locationtech.geomesa.core.process.tube.TubeVisitor
import org.locationtech.geomesa.core.process.unique.AttributeVisitor
import org.opengis.feature.FeatureVisitor
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
import org.opengis.filter.sort.SortBy
import org.opengis.util.ProgressListener

trait AccumuloAbstractFeatureSource extends AbstractFeatureSource {
  self =>

  import org.locationtech.geomesa.utils.geotools.Conversions._

  val dataStore: AccumuloDataStore
  val featureName: Name

  def addFeatureListener(listener: FeatureListener) {}

  def removeFeatureListener(listener: FeatureListener) {}

  def getSchema: SimpleFeatureType = getDataStore.getSchema(featureName)

  def getDataStore: AccumuloDataStore = dataStore

  override def getCount(query: Query) = getFeaturesNoCache(query).features().size

  override def getQueryCapabilities =
    new QueryCapabilities() {
      override def isOffsetSupported = false
      override def isReliableFIDSupported = true
      override def isUseProvidedFIDSupported = true
      override def supportsSorting(sortAttributes: Array[SortBy]) = true
    }

  protected def getFeaturesNoCache(query: Query): SimpleFeatureCollection = {
    AccumuloDataStore.setQueryTransforms(query, getSchema)
    new AccumuloFeatureCollection(self, query)
  }

  override def getFeatures(query: Query): SimpleFeatureCollection = getFeaturesNoCache(query)

  override def getFeatures(filter: Filter): SimpleFeatureCollection = getFeatures(new Query(getSchema().getTypeName, filter))
}

class AccumuloFeatureSource(val dataStore: AccumuloDataStore, val featureName: Name)
  extends AccumuloAbstractFeatureSource

class AccumuloFeatureCollection(source: SimpleFeatureSource, query: Query)
  extends DefaultFeatureResults(source, query) {

  val ds  = source.getDataStore.asInstanceOf[AccumuloDataStore]

  override def getSchema: SimpleFeatureType =
    if(query.getHints.containsKey(TRANSFORMS)) query.getHints.get(TRANSFORM_SCHEMA).asInstanceOf[SimpleFeatureType]
    else super.getSchema

  override def accepts(visitor: FeatureVisitor, progress: ProgressListener) =
    visitor match {
      // TODO GEOMESA-421 implement min/max iterators
      case v: MinVisitor       => v.setValue(new DateTime(2000,1,1,0,0).toDate)
      case v: MaxVisitor       => v.setValue(new DateTime().toDate)
      case v: BoundsVisitor    => v.reset(ds.getBounds(query))
      case v: TubeVisitor      => v.setValue(v.tubeSelect(source, query))
      case v: ProximityVisitor => v.setValue(v.proximitySearch(source, query))
      case v: QueryVisitor     => v.setValue(v.query(source, query))
      case v: KNNVisitor       => v.setValue(v.kNNSearch(source,query))
      case v: AttributeVisitor => v.setValue(v.unique(source, query))
      case _                   => super.accepts(visitor, progress)
    }

  override def reader(): FeatureReader[SimpleFeatureType, SimpleFeature] = super.reader()
}

trait CachingFeatureSource extends AccumuloAbstractFeatureSource {
  self: AccumuloAbstractFeatureSource =>

  private val featureCache =
    CacheBuilder.newBuilder().build(
      new CacheLoader[Query, SimpleFeatureCollection] {
        override def load(query: Query): SimpleFeatureCollection = {
          val accFC = self.getFeaturesNoCache(query)
          new ListFeatureCollection(accFC)
        }
      })

  override def getFeatures(query: Query): SimpleFeatureCollection = {
    // geotools bug in Query.hashCode
    if(query.getStartIndex == null) query.setStartIndex(0)
    featureCache.get(query)
  }

  override def getCount(query: Query): Int = getFeatures(query).size()
}
