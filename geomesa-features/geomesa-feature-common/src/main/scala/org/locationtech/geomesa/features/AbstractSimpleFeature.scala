/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features

import org.geotools.feature.`type`.{AttributeDescriptorImpl, Types}
import org.geotools.feature.{AttributeImpl, GeometryAttributeImpl}
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.utils.geotools.ImmutableFeatureId
import org.locationtech.geomesa.utils.geotools.converters.FastConverter
import org.locationtech.jts.geom.Geometry
import org.opengis.feature.`type`.{AttributeDescriptor, Name}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.feature.{GeometryAttribute, Property}
import org.opengis.filter.identity.FeatureId
import org.opengis.geometry.BoundingBox

/**
  * Base class for simple feature implementations, with boilerplate
  */
object AbstractSimpleFeature {

  /**
    * Base class for immutable simple features
    *
    * @param sft simple feature type
    */
  abstract class AbstractImmutableSimpleFeature(sft: SimpleFeatureType) extends AbstractSimpleFeature(sft) {

    // TODO collection attributes (lists, maps, byte arrays) are still mutable...

    protected var id: String = _ // this is expected to be set by the constructor

    override def getID: String = id
    override def getIdentifier = new ImmutableFeatureId(id)

    override def setAttribute(name: Name, value: Object): Unit = throw new UnsupportedOperationException()
    override def setAttribute(name: String, value: Object): Unit = throw new UnsupportedOperationException()
    override def setAttribute(index: Int, value: Object): Unit = throw new UnsupportedOperationException()
    override def setAttributes(vals: java.util.List[Object]): Unit = throw new UnsupportedOperationException()
    override def setAttributes(vals: Array[Object]): Unit = throw new UnsupportedOperationException()
    override def setDefaultGeometry(geo: Object): Unit = throw new UnsupportedOperationException()
    override def setValue(newValue: Object): Unit = throw new UnsupportedOperationException()
    override def setValue(values: java.util.Collection[Property]): Unit = throw new UnsupportedOperationException()
    override def setDefaultGeometryProperty(geoAttr: GeometryAttribute): Unit = throw new UnsupportedOperationException()
  }

  /**
    * Base class for mutable simple features
    *
    * @param sft simple feature type
    */
  abstract class AbstractMutableSimpleFeature(sft: SimpleFeatureType) extends AbstractSimpleFeature(sft) {

    protected var id: String = _

    def setAttributeNoConvert(index: Int, value: AnyRef): Unit
    def setId(id: String): Unit = this.id = id

    override def getIdentifier: FeatureId = new MutableFeatureIdReference()
    override def getID: String = id

    override def setAttribute(name: Name, value: Object): Unit = setAttribute(name.getLocalPart, value)
    override def setAttribute(name: String, value: Object): Unit = {
      val index = sft.indexOf(name)
      if (index == -1) {
        throw new IllegalArgumentException(s"Attribute $name does not exist in type $sft")
      }
      setAttribute(index, value)
    }
    override def setAttribute(index: Int, value: Object): Unit = {
      if (value == null) {
        setAttributeNoConvert(index, null)
      } else {
        val binding = sft.getDescriptor(index).getType.getBinding
        setAttributeNoConvert(index, FastConverter.convert(value, binding).asInstanceOf[AnyRef])
      }
    }

    // following methods delegate to setAttribute to get type conversion
    override def setAttributes(vals: java.util.List[Object]): Unit = {
      var i = 0
      while (i < vals.size) {
        setAttribute(i, vals.get(i))
        i += 1
      }
    }
    override def setAttributes(vals: Array[Object]): Unit = {
      var i = 0
      while (i < vals.length) {
        setAttribute(i, vals(i))
        i += 1
      }
    }
    override def setDefaultGeometry(geo: Object): Unit = setAttribute(sft.getGeometryDescriptor.getLocalName, geo)
    override def setValue(newValue: Object): Unit = setValue(newValue.asInstanceOf[java.util.Collection[Property]])
    override def setValue(values: java.util.Collection[Property]): Unit = {
      import scala.collection.JavaConversions._
      var i = 0
      values.foreach { p =>
        setAttribute(i, p.getValue)
        i += 1
      }
    }
    override def setDefaultGeometryProperty(geoAttr: GeometryAttribute): Unit =
      if (geoAttr == null) { setDefaultGeometry(null) } else { setDefaultGeometry(geoAttr.getValue) }

    /**
     * Mutable feature ID implementation that references back to the parent class. This allows us to
     * avoid allocating a new object unless `getIdentifier` is called.
     */
    class MutableFeatureIdReference extends FeatureIdImpl("") {
      override def getID: String = id
      override def setID(id: String): Unit = AbstractMutableSimpleFeature.this.id = id
      override def toString: String = id
      override def equals(obj: Any): Boolean = obj match {
        case fid: FeatureId => id == fid.getID
        case _ => false
      }
      override def hashCode: Int = id.hashCode
      override def equalsFID(fid: FeatureId): Boolean = fid != null && fid.getID == id
      override def equalsExact(obj: FeatureId): Boolean =
        equalsFID(obj) && id == obj.getRid && obj.getPreviousRid == null && obj.getFeatureVersion == null
    }
  }
}

/**
  * Base class for simple feature implementations, with boilerplate
  *
  * @param sft simple feature type
  */
abstract class AbstractSimpleFeature(sft: SimpleFeatureType) extends SimpleFeature {

  override def getFeatureType: SimpleFeatureType = sft
  override def getType: SimpleFeatureType = sft
  override def getName: Name = sft.getName

  override def getAttribute(name: Name): AnyRef = getAttribute(name.getLocalPart)
  override def getAttribute(name: String): AnyRef = {
    val index = sft.indexOf(name)
    if (index == -1) { null } else { getAttribute(index) }
  }

  override def getAttributeCount: Int = sft.getAttributeCount
  override def getAttributes: java.util.List[Object] = {
    val list = new java.util.ArrayList[AnyRef](sft.getAttributeCount)
    var i = 0
    while (i < getAttributeCount) {
      list.add(getAttribute(i))
      i += 1
    }
    list
  }

  override def getDefaultGeometry: AnyRef =
    if (sft.getGeometryDescriptor == null) { null } else { getAttribute(sft.getGeometryDescriptor.getLocalName) }

  override def getBounds: BoundingBox = getDefaultGeometry match {
    case g: Geometry => new ReferencedEnvelope(g.getEnvelopeInternal, sft.getCoordinateReferenceSystem)
    case _ => new ReferencedEnvelope(sft.getCoordinateReferenceSystem)
  }

  override def getDefaultGeometryProperty: GeometryAttribute = {
    if (sft.getGeometryDescriptor == null) { null } else {
      new GeometryAttributeImpl(getDefaultGeometry, sft.getGeometryDescriptor, null)
    }
  }

  override def getProperties: java.util.Collection[Property] = {
    val attributes = getAttributes
    val descriptors = sft.getAttributeDescriptors
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
  override def getProperties(name: String): java.util.Collection[Property] = {
    import scala.collection.JavaConversions._
    getProperties.filter(_.getName.toString == name)
  }
  override def getProperty(name: Name): Property = getProperty(name.getLocalPart)
  override def getProperty(name: String): Property = {
    val descriptor = sft.getDescriptor(name)
    if (descriptor == null) null else new AttributeImpl(getAttribute(name), descriptor, getIdentifier)
  }

  override def getValue: java.util.Collection[Property] = getProperties

  override def getDescriptor: AttributeDescriptor =
    new AttributeDescriptorImpl(sft, sft.getName, 0, Int.MaxValue, true, null)

  override def isNillable = true

  override def validate(): Unit = {
    var i = 0
    while (i < getAttributeCount) {
      Types.validate(sft.getDescriptor(i), getAttribute(i))
      i += 1
    }
  }

  override def hashCode: Int = getID.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: SimpleFeature =>
      getID == other.getID && getName == other.getName && getAttributeCount == other.getAttributeCount && {
        var i = 0
        while (i < getAttributeCount) {
          if (getAttribute(i) != other.getAttribute(i)) {
            return false
          }
          i += 1
        }
        true
      }
    case _ => false
  }

  override def toString: String =
    s"${this.getClass.getSimpleName}:$getID:${Seq.tabulate(getAttributeCount)(getAttribute).mkString("|")}"
}
