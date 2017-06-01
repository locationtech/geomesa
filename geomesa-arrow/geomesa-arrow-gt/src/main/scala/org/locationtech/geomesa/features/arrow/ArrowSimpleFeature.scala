/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.arrow

import java.util.{Collection => jCollection, List => jList, Map => jMap}

import com.vividsolutions.jts.geom.Geometry
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.arrow.vector.ArrowAttributeReader
import org.locationtech.geomesa.utils.geotools.ImmutableFeatureId
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.feature.{GeometryAttribute, Property}
import org.opengis.filter.identity.FeatureId
import org.opengis.geometry.BoundingBox

/**
  * Simple feature backed by an arrow vector. Attributes are lazily evaluated - this allows filters to only
  * examine the relevant arrow vectors for optimized reads, but also means that they are tied to the underlying
  * vectors
  *
  * @param sft simple feature type
  * @param idReader id reader
  * @param attributeReaders attribute readers
  * @param index index of the feature in the arrow vector
  */
class ArrowSimpleFeature(sft: SimpleFeatureType,
                         idReader: ArrowAttributeReader,
                         attributeReaders: Array[ArrowAttributeReader],
                         index: Int) extends SimpleFeature {

  import scala.collection.JavaConversions._

  private lazy val id = idReader.apply(index).asInstanceOf[String]
  // in order to try to leverage the columnar memory layout, only read the attributes when requested
  // this way filtering through a single attribute for a bunch of features will hit a contiguous chunk of memory
  private val attributes = attributeReaders.map(a => new ArrowSimpleFeature.Lazy(a.apply(index)))

  private lazy val geomIndex = sft.indexOf(sft.getGeometryDescriptor.getLocalName)

  override def getAttribute(i: Int): AnyRef = attributes(i).value

  /**
    * Load values from the underlying vector, which removes the dependency on it
    */
  def load(): Unit = {
    // just reference the lazy vals so that they are evaluated
    id
    attributes.foreach(_.value)
  }

  override def getID: String = id
  override def getIdentifier: FeatureId = new ImmutableFeatureId(id)

  override def getUserData: jMap[AnyRef, AnyRef] = Map.empty[AnyRef, AnyRef]

  override def getType: SimpleFeatureType = sft
  override def getFeatureType: SimpleFeatureType = sft
  override def getName: Name = sft.getName

  override def getAttribute(name: Name): AnyRef = getAttribute(name.getLocalPart)
  override def getAttribute(name: String): Object = {
    val index = sft.indexOf(name)
    if (index == -1) null else getAttribute(index)
  }

  override def getDefaultGeometry: AnyRef = getAttribute(geomIndex)
  override def getAttributeCount: Int = sft.getAttributeCount

  override def getBounds: BoundingBox = getDefaultGeometry match {
    case g: Geometry => new ReferencedEnvelope(g.getEnvelopeInternal, sft.getCoordinateReferenceSystem)
    case _           => new ReferencedEnvelope(sft.getCoordinateReferenceSystem)
  }

  override def getAttributes: jList[AnyRef] = {
    val attributes = new java.util.ArrayList[AnyRef](sft.getAttributeCount)
    var i = 0
    while (i < sft.getAttributeCount) {
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

  override def setAttribute(name: Name, value: Object) = throw new NotImplementedError
  override def setAttribute(name: String, value: Object) = throw new NotImplementedError
  override def setAttribute(index: Int, value: Object) = throw new NotImplementedError
  override def setAttributes(vals: jList[Object]) = throw new NotImplementedError
  override def setAttributes(vals: Array[Object]) = throw new NotImplementedError
  override def setDefaultGeometry(geo: Object) = throw new NotImplementedError
  override def setDefaultGeometryProperty(geoAttr: GeometryAttribute) = throw new NotImplementedError
  override def setValue(newValue: Object) = throw new NotImplementedError
  override def setValue(values: jCollection[Property]) = throw new NotImplementedError

  override def isNillable = true
  override def validate() = throw new NotImplementedError

  override def toString = s"ArrowSimpleFeature:$getID:${getAttributes.mkString("|")}"

  override def hashCode: Int = getID.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: SimpleFeature =>
      getID == other.getID && getName == other.getName && getAttributes == other.getAttributes
    case _ => false
  }
}

object ArrowSimpleFeature {
  class Lazy[T](v: => T) { lazy val value = v }
}
