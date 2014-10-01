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

package org.locationtech.geomesa.feature

import java.util.{Collection => JCollection, List => JList}

import com.vividsolutions.jts.geom.Geometry
import org.geotools.feature.`type`.{AttributeDescriptorImpl, Types}
import org.geotools.feature.{AttributeImpl, GeometryAttributeImpl}
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.util.Converters
import org.opengis.feature.`type`.{AttributeDescriptor, Name}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.feature.{GeometryAttribute, Property}
import org.opengis.filter.identity.FeatureId
import org.opengis.geometry.BoundingBox

import scala.collection.JavaConversions._
import scala.util.Try


class AvroSimpleFeature(id: FeatureId, sft: SimpleFeatureType)
  extends SimpleFeature
  with Serializable {

  val values = Array.ofDim[AnyRef](sft.getAttributeCount)
  @transient lazy val userData  = collection.mutable.HashMap.empty[AnyRef, AnyRef]

  def getFeatureType = sft
  def getType = sft
  def getIdentifier = id
  def getID = id.getID

  def getAttribute(name: String) = if (sft.indexOf(name) >= 0) getAttribute(sft.indexOf(name)) else null
  def getAttribute(name: Name) = getAttribute(name.getLocalPart)
  def getAttribute(index: Int) = values(index)

  def setAttribute(name: String, value: Object) = setAttribute(sft.indexOf(name), value)
  def setAttribute(name: Name, value: Object) = setAttribute(name.getLocalPart, value)
  def setAttribute(index: Int, value: Object) = setAttributeNoConvert(index,
    Converters.convert(value, getFeatureType.getDescriptor(index).getType.getBinding).asInstanceOf[AnyRef])

  def setAttributes(vals: JList[Object]) = vals.zipWithIndex.foreach { case (v, idx) => setAttribute(idx, v) }
  def setAttributes(vals: Array[Object])= vals.zipWithIndex.foreach { case (v, idx) => setAttribute(idx, v) }

  def setAttributeNoConvert(index: Int, value: Object) = values(index) = value
  def setAttributeNoConvert(name: String, value: Object): Unit = setAttributeNoConvert(sft.indexOf(name), value)
  def setAttributeNoConvert(name: Name, value: Object): Unit = setAttributeNoConvert(name.getLocalPart, value)
  def setAttributesNoConvert(vals: JList[Object]) = vals.zipWithIndex.foreach { case (v, idx) => values(idx) = v }
  def setAttributesNoConvert(vals: Array[Object])= vals.zipWithIndex.foreach { case (v, idx) => values(idx) = v }

  def getAttributeCount = values.length
  def getAttributes: JList[Object] = values.toList
  def getDefaultGeometry: Object = Try(sft.getGeometryDescriptor.getName).map(getAttribute).getOrElse(null)

  def setDefaultGeometry(geo: Object) = setAttribute(sft.getGeometryDescriptor.getName, geo)

  def getBounds: BoundingBox = getDefaultGeometry match {
    case g: Geometry =>
      new ReferencedEnvelope(g.getEnvelopeInternal, sft.getCoordinateReferenceSystem)
    case _ =>
      new ReferencedEnvelope(sft.getCoordinateReferenceSystem)
  }

  def getDefaultGeometryProperty: GeometryAttribute = {
    val geoDesc = sft.getGeometryDescriptor
    geoDesc != null match {
      case true =>
        new GeometryAttributeImpl(getDefaultGeometry, geoDesc, null)
      case false =>
        null
    }
  }

  def setDefaultGeometryProperty(geoAttr: GeometryAttribute) = geoAttr != null match {
    case true =>
      setDefaultGeometry(geoAttr.getValue)
    case false =>
      setDefaultGeometry(null)
  }

  def getProperties: JCollection[Property] =
    getAttributes.zip(sft.getAttributeDescriptors).map {
      case(attribute, attributeDescriptor) =>
         new AttributeImpl(attribute, attributeDescriptor, id)
      }
  def getProperties(name: Name): JCollection[Property] = getProperties(name.getLocalPart)
  def getProperties(name: String): JCollection[Property] = getProperties.filter(_.getName.toString == name)
  def getProperty(name: Name): Property = getProperty(name.getLocalPart)
  def getProperty(name: String): Property =
    Option(sft.getDescriptor(name)) match {
      case Some(descriptor) => new AttributeImpl(getAttribute(name), descriptor, id)
      case _ => null
    }

  def getValue: JCollection[_ <: Property] = getProperties

  def setValue(values: JCollection[Property]) = values.zipWithIndex.foreach { case (p, idx) =>
    this.values(idx) = p.getValue
  }

  def getDescriptor: AttributeDescriptor = new AttributeDescriptorImpl(sft, sft.getName, 0, Int.MaxValue, true, null)

  def getName: Name = sft.getName

  def getUserData = userData

  def isNillable = true

  def setValue(newValue: Object) = setValue (newValue.asInstanceOf[JCollection[Property]])

  def validate() = values.zipWithIndex.foreach { case (v, idx) => Types.validate(getType.getDescriptor(idx), v) }

}
