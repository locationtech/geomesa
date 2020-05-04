/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features

import java.util.{Collection => jCollection, List => jList, Map => jMap}

import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.utils.geotools.Transform
import org.locationtech.geomesa.utils.geotools.Transform.Transforms
import org.locationtech.geomesa.utils.io.Sizable
import org.locationtech.jts.geom.Geometry
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.feature.{GeometryAttribute, Property}
import org.opengis.filter.identity.FeatureId
import org.opengis.geometry.BoundingBox

/**
  * Simple feature implementation that wraps another feature type and applies a transform/projection
  *
  * @param transformSchema transformed feature type
  * @param attributes attribute evaluations, in order
  */
class TransformSimpleFeature(
    transformSchema: SimpleFeatureType,
    attributes: Array[Transform],
    private var underlying: SimpleFeature = null
  ) extends SimpleFeature with Sizable {

  private lazy val geomIndex = transformSchema.indexOf(transformSchema.getGeometryDescriptor.getLocalName)

  def setFeature(sf: SimpleFeature): TransformSimpleFeature = {
    underlying = sf
    this
  }

  override def calculateSizeOf(): Long = underlying match {
    case s: Sizable => s.calculateSizeOf()
    case _ => Sizable.sizeOf(underlying) + Sizable.deepSizeOf(underlying.getAttributes, underlying.getUserData)
  }

  override def getAttribute(index: Int): AnyRef = attributes(index).evaluate(underlying)

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

  def apply(
      sft: SimpleFeatureType,
      transformSchema: SimpleFeatureType,
      transforms: String): TransformSimpleFeature = {
    new TransformSimpleFeature(transformSchema, Transforms(sft, transforms).toArray)
  }

  def apply(transformSchema: SimpleFeatureType, transforms: Seq[Transform]): TransformSimpleFeature =
    new TransformSimpleFeature(transformSchema, transforms.toArray)

  @deprecated("replaced with org.locationtech.geomesa.utils.geotools.Transform")
  def attributes(sft: SimpleFeatureType, transforms: String): Array[SimpleFeature => AnyRef] =
    Transforms(sft, transforms).map(t => t.evaluate _).toArray
}
