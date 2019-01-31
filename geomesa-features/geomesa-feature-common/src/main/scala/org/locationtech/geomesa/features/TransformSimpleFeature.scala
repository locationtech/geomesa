/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features

import java.util.{Collection => jCollection, List => jList, Map => jMap}

import org.locationtech.jts.geom.Geometry
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.process.vector.TransformProcess
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.feature.{GeometryAttribute, Property}
import org.opengis.filter.expression.{Expression, PropertyName}
import org.opengis.filter.identity.FeatureId
import org.opengis.geometry.BoundingBox

/**
  * Simple feature implementation that wraps another feature type and applies a transform/projection
  *
  * @param transformSchema transformed feature type
  * @param attributes attribute evaluations, in order
  */
class TransformSimpleFeature(transformSchema: SimpleFeatureType,
                             attributes: Array[SimpleFeature => AnyRef],
                             private var underlying: SimpleFeature = null) extends SimpleFeature {

  private lazy val geomIndex = transformSchema.indexOf(transformSchema.getGeometryDescriptor.getLocalName)

  def setFeature(sf: SimpleFeature): TransformSimpleFeature = {
    underlying = sf
    this
  }

  override def getAttribute(index: Int): AnyRef = attributes(index).apply(underlying)

  override def getIdentifier: FeatureId = underlying.getIdentifier
  override def getID: String = underlying.getID

  override def getUserData: jMap[AnyRef, AnyRef] = underlying.getUserData

  override def getType: SimpleFeatureType = transformSchema
  override def getFeatureType: SimpleFeatureType = transformSchema
  override def getName: Name = transformSchema.getName

  override def getAttribute(name: Name): AnyRef = getAttribute(name.getLocalPart)
  override def getAttribute(name: String): Object = {
    val index = transformSchema.indexOf(name)
    if (index == -1) null else getAttribute(index)
  }

  override def getDefaultGeometry: AnyRef = getAttribute(geomIndex)
  override def getAttributeCount: Int = transformSchema.getAttributeCount

  override def getBounds: BoundingBox = getDefaultGeometry match {
    case g: Geometry => new ReferencedEnvelope(g.getEnvelopeInternal, transformSchema.getCoordinateReferenceSystem)
    case _           => new ReferencedEnvelope(transformSchema.getCoordinateReferenceSystem)
  }

  override def getAttributes: jList[AnyRef] = {
    val attributes = new java.util.ArrayList[AnyRef](transformSchema.getAttributeCount)
    var i = 0
    while (i < transformSchema.getAttributeCount) {
      attributes.add(getAttribute(i))
      i += 1
    }
    attributes
  }

  override def getDefaultGeometryProperty = throw new NotImplementedError
  override def getProperties: jCollection[Property] = throw new NotImplementedError
  override def getProperties(name: Name) = throw new NotImplementedError
  override def getProperties(name: String) = throw new NotImplementedError
  override def getProperty(name: Name) = throw new NotImplementedError
  override def getProperty(name: String) = throw new NotImplementedError
  override def getValue = throw new NotImplementedError
  override def getDescriptor = throw new NotImplementedError

  override def setAttribute(name: Name, value: Object): Unit = throw new NotImplementedError
  override def setAttribute(name: String, value: Object): Unit = throw new NotImplementedError
  override def setAttribute(index: Int, value: Object): Unit = throw new NotImplementedError
  override def setAttributes(vals: jList[Object]): Unit = throw new NotImplementedError
  override def setAttributes(vals: Array[Object]): Unit = throw new NotImplementedError
  override def setDefaultGeometry(geo: Object): Unit = throw new NotImplementedError
  override def setDefaultGeometryProperty(geoAttr: GeometryAttribute): Unit = throw new NotImplementedError
  override def setValue(newValue: Object): Unit = throw new NotImplementedError
  override def setValue(values: jCollection[Property]): Unit = throw new NotImplementedError

  override def isNillable: Boolean = true
  override def validate(): Unit = throw new NotImplementedError

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

  override def toString = s"TransformSimpleFeature:$getID"
}

object TransformSimpleFeature {

  import scala.collection.JavaConversions._

  def apply(sft: SimpleFeatureType, transformSchema: SimpleFeatureType, transforms: String): TransformSimpleFeature = {
    val a = attributes(sft, transformSchema, transforms)
    new TransformSimpleFeature(transformSchema, a)
  }

  def attributes(sft: SimpleFeatureType,
                 transformSchema: SimpleFeatureType,
                 transforms: String): Array[SimpleFeature => AnyRef] = {
    TransformProcess.toDefinition(transforms).map(attribute(sft, _)).toArray
  }

  private def attribute(sft: SimpleFeatureType, d: TransformProcess.Definition): SimpleFeature => AnyRef = {
    d.expression match {
      case p: PropertyName => val i = sft.indexOf(p.getPropertyName); sf => sf.getAttribute(i)
      case e: Expression   => sf => e.evaluate(sf)
    }
  }
}
