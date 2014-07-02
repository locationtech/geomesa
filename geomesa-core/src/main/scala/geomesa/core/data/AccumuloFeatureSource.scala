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

import geomesa.core.process.knn.KNNVisitor

import collection.JavaConversions._
import geomesa.core.process.proximity.ProximityVisitor
import geomesa.core.process.query.QueryVisitor
import geomesa.core.process.tube.TubeVisitor
import org.geotools.data._
import org.geotools.data.simple.{SimpleFeatureSource, SimpleFeatureCollection}
import org.geotools.feature.visitor.{BoundsVisitor, MaxVisitor, MinVisitor}
import org.geotools.process.vector.TransformProcess
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


  override def getFeatures(query: Query): SimpleFeatureCollection = {
    if(query.getProperties != null && query.getProperties.size > 0) {
      val (transformProps, regularProps) = query.getPropertyNames.partition(_.contains('='))
      val convertedRegularProps = regularProps.map { p => s"$p=$p" }
      val allTransforms = convertedRegularProps ++ transformProps
      val transforms = allTransforms.mkString(";")
      val transformDefs = TransformProcess.toDefinition(transforms)
      val derivedSchema = AccumuloFeatureStore.computeSchema(getSchema, transformDefs)
      query.setProperties(Query.ALL_PROPERTIES)
      query.getHints.put(TRANSFORMS, transforms)
      query.getHints.put(TRANSFORM_SCHEMA, derivedSchema)
    }
    new AccumuloFeatureCollection(this, query)
  }

  override def getFeatures(filter: Filter): SimpleFeatureCollection =
    getFeatures(new Query(getSchema().getTypeName, filter))
}

class AccumuloFeatureSource(val dataStore: AccumuloDataStore, val featureName: String)
  extends AccumuloAbstractFeatureSource

class AccumuloFeatureCollection(source: SimpleFeatureSource,
                                query: Query)
  extends DefaultFeatureResults(source, query) {

  val ds  = source.getDataStore.asInstanceOf[AccumuloDataStore]

  override def getSchema: SimpleFeatureType =
    if(query.getHints.containsKey(TRANSFORMS)) query.getHints.get(TRANSFORM_SCHEMA).asInstanceOf[SimpleFeatureType]
    else super.getSchema

  override def accepts(visitor: FeatureVisitor, progress: ProgressListener) = visitor match {
    // TODO: implement min/max iterators
    case v: MinVisitor       => v.setValue(new DateTime(2000,1,1,0,0).toDate)
    case v: MaxVisitor       => v.setValue(new DateTime().toDate)
    case v: BoundsVisitor    => v.reset(ds.getBounds(query))
    case v: TubeVisitor      => v.setValue(v.tubeSelect(source, query))
    case v: ProximityVisitor => v.setValue(v.proximitySearch(source, query))
    case v: QueryVisitor     => v.setValue(v.query(source, query))
    case v: KNNVisitor       => v.setValue(v.kNNSearch(source,query))
    case _                   => super.accepts(visitor, progress)
  }

}