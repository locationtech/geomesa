/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.feature.kryo

import java.util.{Collection => JCollection, List => JList}

import com.vividsolutions.jts.geom.Geometry
import org.geotools.feature.`type`.{AttributeDescriptorImpl, Types}
import org.geotools.feature.{AttributeImpl, GeometryAttributeImpl}
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.util.Converters
import org.opengis.feature.`type`.{AttributeDescriptor, Name}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.feature.{GeometryAttribute, Property}
import org.opengis.geometry.BoundingBox

import scala.collection.JavaConversions._

/**
 * Simple feature implementation optimized to instantiate from kryo serialization
 *
 * @param initialId
 * @param sft
 * @param initialValues if provided, must already be converted into the appropriate types
 */
class KryoSimpleFeature(initialId: String, sft: SimpleFeatureType, initialValues: Array[AnyRef] = null)
    extends SimpleFeature {

  val featureId = new FeatureIdImpl(initialId)
  val values = if (initialValues == null) Array.ofDim[AnyRef](sft.getAttributeCount) else initialValues

  lazy private[this] val userData  = collection.mutable.HashMap.empty[AnyRef, AnyRef]
  lazy private[this] val geometryDescriptor = sft.getGeometryDescriptor

  override def getFeatureType = sft
  override def getType = sft
  override def getIdentifier = featureId
  override def getID = featureId.getID // this needs to reference the featureId, as it can be updated

  override def getAttribute(name: Name) = getAttribute(name.getLocalPart)
  override def getAttribute(name: String) = {
    val index = sft.indexOf(name)
    if (index == -1) null else getAttribute(index)
  }
  override def getAttribute(index: Int) = values(index)

  override def setAttribute(name: Name, value: Object) = setAttribute(name.getLocalPart, value)
  override def setAttribute(name: String, value: Object) = {
    val index = sft.indexOf(name)
    if (index == -1) {
      throw new IllegalArgumentException(s"Attribute $name does not exist in type $sft")
    }
    setAttribute(index, value)
  }
  override def setAttribute(index: Int, value: Object) = {
    val binding = sft.getDescriptor(index).getType.getBinding
    values(index) = Converters.convert(value, binding).asInstanceOf[AnyRef]
  }

  override def setAttributes(vals: JList[Object]) =
    vals.zipWithIndex.foreach { case (v, i) => setAttribute(i, v) }
  override def setAttributes(vals: Array[Object]) =
    vals.zipWithIndex.foreach { case (v, i) => setAttribute(i, v) }

  override def getAttributeCount = values.length
  override def getAttributes: JList[Object] = values.toList

  override def getDefaultGeometry: Object =
    if (geometryDescriptor == null) null else getAttribute(geometryDescriptor.getLocalName)
  override def setDefaultGeometry(geo: Object) = setAttribute(geometryDescriptor.getName, geo)

  override def getBounds: BoundingBox = getDefaultGeometry match {
    case g: Geometry =>
      new ReferencedEnvelope(g.getEnvelopeInternal, sft.getCoordinateReferenceSystem)
    case _ =>
      new ReferencedEnvelope(sft.getCoordinateReferenceSystem)
  }

  override def getDefaultGeometryProperty: GeometryAttribute =
    if (geometryDescriptor == null) null else new GeometryAttributeImpl(getDefaultGeometry, geometryDescriptor, null)

  override def setDefaultGeometryProperty(geoAttr: GeometryAttribute) =
    if (geoAttr == null) setDefaultGeometry(null) else setDefaultGeometry(geoAttr.getValue)

  override def getProperties: JCollection[Property] =
    getAttributes.zip(sft.getAttributeDescriptors).map {
      case(attribute, attributeDescriptor) =>
         new AttributeImpl(attribute, attributeDescriptor, featureId)
      }
  override def getProperties(name: Name): JCollection[Property] = getProperties(name.getLocalPart)
  override def getProperties(name: String): JCollection[Property] = getProperties.filter(_.getName.toString == name)
  override def getProperty(name: Name): Property = getProperty(name.getLocalPart)
  override def getProperty(name: String): Property = {
    val descriptor = sft.getDescriptor(name)
    if (descriptor == null) null else new AttributeImpl(getAttribute(name), descriptor, featureId)
  }

  override def getValue: JCollection[_ <: Property] = getProperties
  override def setValue(newValue: Object) = setValue (newValue.asInstanceOf[JCollection[Property]])
  override def setValue(values: JCollection[Property]) =
    values.zipWithIndex.foreach { case (p, idx) => this.values(idx) = p.getValue }

  override def getDescriptor: AttributeDescriptor =
    new AttributeDescriptorImpl(sft, sft.getName, 0, Int.MaxValue, true, null)

  override def getName: Name = sft.getName

  override def getUserData = userData

  override def isNillable = true

  override def validate() =
    values.zipWithIndex.foreach { case (v, idx) => Types.validate(getType.getDescriptor(idx), v) }
}

object KryoSimpleFeature {
  implicit class RichKryoSimpleFeature(val sf: KryoSimpleFeature) extends AnyVal {
    def getAttribute[T](name: String) = sf.getAttribute(name).asInstanceOf[T]
    def getAttribute[T](index: Int) = sf.getAttribute(index).asInstanceOf[T]
    def getGeometry() = sf.getDefaultGeometry.asInstanceOf[Geometry]
  }
}