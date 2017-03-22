/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features

import java.util.{Collections, Collection => JCollection, HashMap => JHashMap, List => JList}

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
 * Simple feature implementation optimized to instantiate from serialization
 *
 * @param initialId simple feature id
 * @param sft simple feature type
 * @param initialValues if provided, must already be converted into the appropriate types
 */
class ScalaSimpleFeature(initialId: String,
                         sft: SimpleFeatureType,
                         initialValues: Array[AnyRef] = null,
                         initialUserData: java.util.Map[AnyRef, AnyRef] = null)
    extends SimpleFeature {

  val featureId = new FeatureIdImpl(initialId)
  val values = if (initialValues == null) Array.ofDim[AnyRef](sft.getAttributeCount) else initialValues

  lazy private[this] val geomDesc  = sft.getGeometryDescriptor
  lazy private[this] val geomIndex = if (geomDesc == null) -1 else sft.indexOf(geomDesc.getLocalName)
  lazy private[this] val userData  =
    if (initialUserData == null) new JHashMap[AnyRef, AnyRef]() else new JHashMap[AnyRef, AnyRef](initialUserData)

  override def getFeatureType = sft
  override def getType = sft
  override def getIdentifier = featureId
  override def getID = featureId.getID // this needs to reference the featureId, as it can be updated
  override def getName = sft.getName
  override def getUserData = userData

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

  // following methods delegate to setAttribute to get type conversion
  override def setAttributes(vals: JList[Object]) = {
    var i = 0
    while (i < vals.size) {
      setAttribute(i, vals.get(i))
      i += 1
    }
  }
  override def setAttributes(vals: Array[Object]) = {
    var i = 0
    while (i < vals.length) {
      setAttribute(i, vals(i))
      i += 1
    }
  }

  override def getAttributeCount = values.length
  override def getAttributes: JList[Object] = Collections.unmodifiableList(java.util.Arrays.asList(values: _*))

  override def getDefaultGeometry: Object = if (geomIndex == -1) null else getAttribute(geomIndex)
  override def setDefaultGeometry(geo: Object) = setAttribute(geomIndex, geo)

  override def getBounds: BoundingBox = getDefaultGeometry match {
    case g: Geometry => new ReferencedEnvelope(g.getEnvelopeInternal, sft.getCoordinateReferenceSystem)
    case _ => new ReferencedEnvelope(sft.getCoordinateReferenceSystem)
  }

  override def getDefaultGeometryProperty =
    if (geomDesc == null) null else new GeometryAttributeImpl(getDefaultGeometry, geomDesc, null)
  override def setDefaultGeometryProperty(geoAttr: GeometryAttribute) =
    if (geoAttr == null) setDefaultGeometry(null) else setDefaultGeometry(geoAttr.getValue)

  override def getProperties: JCollection[Property] = {
    val attributes = getAttributes
    val descriptors = sft.getAttributeDescriptors
    assert(attributes.size == descriptors.size)
    val properties = new java.util.ArrayList[Property](attributes.size)
    var i = 0
    while (i < attributes.size) {
      properties.add(new AttributeImpl(attributes.get(i), descriptors.get(i), featureId))
      i += 1
    }
    properties
  }
  override def getProperties(name: Name) = getProperties(name.getLocalPart)
  override def getProperties(name: String) = getProperties.filter(_.getName.toString == name)
  override def getProperty(name: Name) = getProperty(name.getLocalPart)
  override def getProperty(name: String) = {
    val descriptor = sft.getDescriptor(name)
    if (descriptor == null) null else new AttributeImpl(getAttribute(name), descriptor, featureId)
  }

  override def getValue = getProperties
  override def setValue(newValue: Object) = setValue(newValue.asInstanceOf[JCollection[Property]])
  override def setValue(values: JCollection[Property]) = {
    var i = 0
    values.foreach { p =>
      setAttribute(i, p.getValue)
      i += 1
    }
  }

  override def getDescriptor: AttributeDescriptor =
    new AttributeDescriptorImpl(sft, sft.getName, 0, Int.MaxValue, true, null)

  override def isNillable = true

  override def validate() = {
    var i = 0
    while (i < values.length) {
      Types.validate(sft.getDescriptor(i), values(i))
      i += 1
    }
  }

  override def toString = s"ScalaSimpleFeature:$getID"

  override def hashCode: Int = getID.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: ScalaSimpleFeature if getIdentifier.equalsExact(other.getIdentifier) =>
      getName == other.getName && java.util.Arrays.equals(values, other.getAttributes.toArray)
    case _ => false
  }
}

object ScalaSimpleFeature {

  def create(sft: SimpleFeatureType, copy: SimpleFeature): ScalaSimpleFeature =
    new ScalaSimpleFeature(copy.getID, sft, copy.getAttributes.toArray, copy.getUserData)

  /**
   * Creates a simple feature, converting the values to the appropriate type
   */
  def create(sft: SimpleFeatureType, id: String, values: Any*): ScalaSimpleFeature = {
    val sf = new ScalaSimpleFeature(id, sft)
    var i = 0
    while (i < values.length) {
      sf.setAttribute(i, values(i).asInstanceOf[AnyRef])
      i += 1
    }
    sf
  }

  /**
   * Compares the id and attributes for the simple features - concrete class is not checked
   */
  def equalIdAndAttributes(sf1: SimpleFeature, sf2: SimpleFeature): Boolean =
    sf1 != null && sf2 != null && sf1.getIdentifier.equalsExact(sf2.getIdentifier) &&
        java.util.Arrays.equals(sf1.getAttributes.toArray, sf2.getAttributes.toArray)
}