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

import java.util.{List => JList, Map => JMap, Set => JSet}

import com.vividsolutions.jts.geom.Geometry
import org.geotools.data._
import org.geotools.feature._
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.filter.FunctionExpressionImpl
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.process.vector.TransformProcess.Definition
import org.opengis.feature.GeometryAttribute
import org.opengis.feature.`type`.{AttributeDescriptor, GeometryDescriptor}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.expression.PropertyName
import org.opengis.filter.identity.FeatureId

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
          val descriptor = if (origAttr.isInstanceOf[GeometryDescriptor]) {
            ab.buildDescriptor(name, ab.buildGeometryType())
          } else {
            ab.buildDescriptor(name, ab.buildType())
          }
          descriptor.getUserData.putAll(origAttr.getUserData)
          descriptor

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