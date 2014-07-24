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

import com.vividsolutions.jts.geom.Geometry
import geomesa.core._
import geomesa.core.conf._
import geomesa.core.data.mapreduce.FeatureIngestMapper.FeatureIngestMapper
import geomesa.utils.geotools.FeatureHandler
import java.io.File
import java.io.Serializable
import java.util.{List => JList, Set => JSet, Map => JMap, UUID}
import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat
import org.apache.accumulo.core.data.{Value, Key}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Reducer, Job}
import org.geotools.data._
import org.geotools.data.store._
import org.geotools.feature._
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.filter.FunctionExpressionImpl
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.process.vector.TransformProcess.Definition
import org.opengis.feature.GeometryAttribute
import org.opengis.feature.`type`.{GeometryDescriptor, AttributeDescriptor}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.expression.PropertyName
import org.opengis.filter.identity.FeatureId
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class AccumuloFeatureStore(val dataStore: AccumuloDataStore, val featureName: String)
    extends AbstractFeatureStore with AccumuloAbstractFeatureSource {
  override def addFeatures(collection: FeatureCollection[SimpleFeatureType, SimpleFeature]): JList[FeatureId] = {
    writeBounds(collection.getBounds)
    super.addFeatures(collection)
  }

  def writeBounds(envelope: ReferencedEnvelope) {
    if(envelope != null)
      dataStore.writeBounds(featureName, envelope)
  }
}

object MapReduceAccumuloFeatureStore {
  val MAPRED_CLASSPATH_USER_PRECEDENCE_KEY = "mapreduce.task.classpath.user.precedence"
}

object AccumuloFeatureStore {

  def computeSchema(origSFT: SimpleFeatureType, transforms: Seq[Definition]): SimpleFeatureType = {
    val attributes: Seq[AttributeDescriptor] = transforms.map { definition =>
      val name = definition.name
      val cql  = definition.expression
      cql match {
        case p: PropertyName =>
          val origAttr = origSFT.getDescriptor(p.getPropertyName)
          val ab = new AttributeTypeBuilder()
          ab.init(origAttr)
          if(origAttr.isInstanceOf[GeometryDescriptor]) {
            ab.buildDescriptor(name, ab.buildGeometryType())
          } else {
            ab.buildDescriptor(name, ab.buildType())
          }

        case f: FunctionExpressionImpl  =>
          val clazz = f.getFunctionName.getReturn.getType
          val ab = new AttributeTypeBuilder().binding(clazz)
          if(classOf[Geometry].isAssignableFrom(clazz))
            ab.buildDescriptor(name, ab.buildGeometryType())
          else
            ab.buildDescriptor(name, ab.buildType())

      }
    }

    val geomAttributes = attributes.filter { _.isInstanceOf[GeometryAttribute] }
    val sftBuilder = new SimpleFeatureTypeBuilder()
    sftBuilder.setName(origSFT.getName)
    sftBuilder.addAll(attributes.toArray)
    if(geomAttributes.size > 0) {
      val defaultGeom =
        if(geomAttributes.size == 1) geomAttributes.head.getLocalName
        else {
          // try to find a geom with the same name as the original default geom
          val origDefaultGeom = origSFT.getGeometryDescriptor.getLocalName
          geomAttributes.find(_.getLocalName.equals(origDefaultGeom))
            .map(_.getLocalName)
            .getOrElse(geomAttributes.head.getLocalName)
        }
      sftBuilder.setDefaultGeometry(defaultGeom)
    }
    val schema = sftBuilder.buildFeatureType()
    schema.getUserData.putAll(origSFT.getUserData)
    schema
  }
}