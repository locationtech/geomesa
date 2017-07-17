/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features

import java.util.Collections

import com.vividsolutions.jts.geom.Geometry
import org.geotools.feature.`type`.{AttributeDescriptorImpl, Types}
import org.geotools.feature.{AttributeImpl, GeometryAttributeImpl}
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.utils.geotools.ImmutableFeatureId
import org.opengis.feature.`type`.{AttributeDescriptor, Name}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.feature.{GeometryAttribute, Property}
import org.opengis.geometry.BoundingBox

import scala.collection.JavaConversions._

/**
  * Immutable simple feature implementation
  *
  * @param getFeatureType simple feature type
  * @param getID simple feature id
  * @param values attribute values, must already be converted into the appropriate types
  * @param initialUserData user data, or null
  */
class ImmutableSimpleFeature(override val getFeatureType: SimpleFeatureType,
                             override val getID: String,
                             private val values: Array[AnyRef],
                             initialUserData: java.util.Map[AnyRef, AnyRef] = null) extends SimpleFeature {

  override val getIdentifier = new ImmutableFeatureId(getID)
  override val getUserData: java.util.Map[AnyRef, AnyRef] =
    if (initialUserData == null) {
      Collections.EMPTY_MAP.asInstanceOf[java.util.Map[AnyRef, AnyRef]]
    } else {
      Collections.unmodifiableMap(initialUserData)
    }

  lazy private val geomDesc  = getFeatureType.getGeometryDescriptor
  lazy private val geomIndex = if (geomDesc == null) { -1 } else { getFeatureType.indexOf(geomDesc.getLocalName) }

  override def getType: SimpleFeatureType = getFeatureType
  override def getName: Name = getFeatureType.getName
  override def getAttributeCount: Int = values.length

  override def getAttribute(name: Name): AnyRef = getAttribute(name.getLocalPart)
  override def getAttribute(name: String): AnyRef = {
    val index = getFeatureType.indexOf(name)
    if (index == -1) { null } else { getAttribute(index) }
  }
  override def getAttribute(index: Int): AnyRef = values(index)
  override def getAttributes: java.util.List[Object] = Collections.unmodifiableList(java.util.Arrays.asList(values: _*))
  override def getDefaultGeometry: AnyRef = if (geomIndex == -1) { null } else { getAttribute(geomIndex) }

  override def getBounds: BoundingBox = getDefaultGeometry match {
    case g: Geometry => new ReferencedEnvelope(g.getEnvelopeInternal, getFeatureType.getCoordinateReferenceSystem)
    case _ => new ReferencedEnvelope(getFeatureType.getCoordinateReferenceSystem)
  }

  override def getDefaultGeometryProperty: GeometryAttribute =
    if (geomDesc == null) { null } else { new GeometryAttributeImpl(getDefaultGeometry, geomDesc, null) }

  override def getProperties: java.util.Collection[Property] = {
    val attributes = getAttributes
    val descriptors = getFeatureType.getAttributeDescriptors
    assert(attributes.size == descriptors.size)
    val properties = new java.util.ArrayList[Property](attributes.size)
    var i = 0
    while (i < attributes.size) {
      properties.add(new AttributeImpl(attributes.get(i), descriptors.get(i), getIdentifier))
      i += 1
    }
    properties
  }
  override def getProperties(name: Name): java.util.Collection[Property] = getProperties(name.getLocalPart)
  override def getProperties(name: String): java.util.Collection[Property] = getProperties.filter(_.getName.toString == name)
  override def getProperty(name: Name): Property = getProperty(name.getLocalPart)
  override def getProperty(name: String): Property = {
    val descriptor = getFeatureType.getDescriptor(name)
    if (descriptor == null) { null } else { new AttributeImpl(getAttribute(name), descriptor, getIdentifier) }
  }

  override def getValue: java.util.Collection[Property] = getProperties

  override def getDescriptor: AttributeDescriptor =
    new AttributeDescriptorImpl(getFeatureType, getFeatureType.getName, 0, Int.MaxValue, true, null)

  override def isNillable: Boolean = true

  override def validate(): Unit = {
    var i = 0
    while (i < values.length) {
      Types.validate(getFeatureType.getDescriptor(i), values(i))
      i += 1
    }
  }

  override def setAttribute(name: Name, value: Object): Unit = throw new UnsupportedOperationException()
  override def setAttribute(name: String, value: Object): Unit = throw new UnsupportedOperationException()
  override def setAttribute(index: Int, value: Object): Unit = throw new UnsupportedOperationException()
  override def setAttributes(vals: java.util.List[Object]): Unit = throw new UnsupportedOperationException()
  override def setAttributes(vals: Array[Object]): Unit = throw new UnsupportedOperationException()
  override def setDefaultGeometry(geo: Object): Unit = throw new UnsupportedOperationException()
  override def setValue(newValue: Object): Unit = throw new UnsupportedOperationException()
  override def setValue(values: java.util.Collection[Property]): Unit = throw new UnsupportedOperationException()
  override def setDefaultGeometryProperty(geoAttr: GeometryAttribute): Unit = throw new UnsupportedOperationException()

  override def toString = s"ImmutableSimpleFeature:$getID:${getAttributes.mkString("|")}"

  override def hashCode: Int = getID.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: ImmutableSimpleFeature =>
      getID == other.getID && getName == other.getName && java.util.Arrays.equals(values, other.values)
    case other: ScalaSimpleFeature =>
      getID == other.getID && getName == other.getName && java.util.Arrays.equals(values, other.values)
    case other: SimpleFeature =>
      getID == other.getID && getName == other.getName && java.util.Arrays.equals(values, other.getAttributes.toArray)
    case _ => false
  }
}

