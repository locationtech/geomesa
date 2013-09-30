/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
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

package geomesa.core.data

import org.geotools.data._
import org.geotools.data.simple.{SimpleFeatureSource, SimpleFeatureCollection}
import org.geotools.feature.visitor.{BoundsVisitor, MaxVisitor, MinVisitor}
import org.joda.time.DateTime
import org.opengis.feature.FeatureVisitor
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter
import org.opengis.util.ProgressListener

trait AccumuloAbstractFeatureSource extends AbstractFeatureSource {
  val dataStore: AccumuloDataStore
  val featureName: String

  def addFeatureListener(listener: FeatureListener) {}

  def removeFeatureListener(listener: FeatureListener) {}

  def getSchema: SimpleFeatureType = getDataStore.getSchema(featureName)

  def getDataStore: AccumuloDataStore = dataStore

  override def getCount(query: Query) = {
    val reader = getFeatures(query).features
    var count = 0
    while (reader.hasNext) {
      reader.next()
      count += 1
    }
    reader.close()
    count
  }

  override def getQueryCapabilities() = {
    new QueryCapabilities(){
      override def isOffsetSupported = false

      override def isReliableFIDSupported = true

      override def isUseProvidedFIDSupported = true
    }
  }

  override def getFeatures(query: Query): SimpleFeatureCollection =
    new AccumuloFeatureCollection(this, query)

  override def getFeatures(filter: Filter): SimpleFeatureCollection =
    getFeatures(new Query(getSchema().getTypeName(), filter))
}

class AccumuloFeatureSource(val dataStore: AccumuloDataStore, val featureName: String)
  extends AccumuloAbstractFeatureSource

class AccumuloFeatureCollection(source: SimpleFeatureSource, query: Query)
  extends DefaultFeatureResults(source, query) {

  override def accepts(visitor: FeatureVisitor, progress: ProgressListener) = visitor match {
    // TODO: implement min/max iterators
    case v: MinVisitor =>
      v.setValue(new DateTime(2000,1,1,0,0).toDate)
    case v: MaxVisitor =>
      v.setValue(new DateTime().toDate)
    case v: BoundsVisitor =>
      val bounds = source.getDataStore.asInstanceOf[AccumuloDataStore].getBounds(query)
      v.reset(bounds)
    case _ => super.accepts(visitor, progress)
  }

}