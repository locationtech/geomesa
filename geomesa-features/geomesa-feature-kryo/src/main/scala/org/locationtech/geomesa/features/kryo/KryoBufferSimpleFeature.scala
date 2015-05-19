/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.features.kryo

import java.util.{Collection => jCollection, HashMap => jHashMap, List => jList, Map => jMap}

import com.esotericsoftware.kryo.io.Input
import com.vividsolutions.jts.geom.Geometry
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.process.vector.TransformProcess
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.feature.{GeometryAttribute, Property}
import org.opengis.filter.expression.PropertyName
import org.opengis.filter.identity.FeatureId
import org.opengis.geometry.BoundingBox

import scala.collection.JavaConversions._

object LazySimpleFeature {
  val NULL_BYTE = 0.asInstanceOf[Byte]
}

class KryoBufferSimpleFeature(sft: SimpleFeatureType, readers: Array[(Input) => AnyRef]) extends SimpleFeature {

  private val input = new Input
  private val offsets = Array.ofDim[Int](sft.getAttributeCount)
  private lazy val geomIndex = sft.indexOf(sft.getGeometryDescriptor.getLocalName)
  private var userData: jHashMap[AnyRef, AnyRef] = null

  private var binaryTransform: () => Array[Byte] = input.getBuffer

  def transform(): Array[Byte] = binaryTransform()

  def setBuffer(bytes: Array[Byte]) = {
    input.setBuffer(bytes)
    // reset our offsets
    input.setPosition(1) // skip version
    input.setPosition(input.readInt()) // set to offsets start
    var i = 0
    while (i < offsets.length) {
      offsets(i) = input.readInt(true)
      i += 1
    }
    userData = null
  }

  def setTransforms(transforms: String, transformSchema: SimpleFeatureType) = {
    val tdefs = TransformProcess.toDefinition(transforms)
    val isSimpleMapping = tdefs.forall(_.expression.isInstanceOf[PropertyName])
    binaryTransform = if (isSimpleMapping) {
      // simple mapping of existing fields - we can array copy the existing data without parsing it
      val indices = transformSchema.getAttributeDescriptors.map(d => sft.indexOf(d.getLocalName))
      () => {
        val buf = input.getBuffer
        var length = offsets(0) // space for version, offset block and ID
        val offsetsAndLengths = indices.map { i =>
            val l = (if (i < offsets.length - 1) offsets(i + 1) else buf.length) - offsets(i)
            length += l
            (offsets(i), l)
          }
        val dst = Array.ofDim[Byte](length)
        // copy the version, offset block and id - offset block isn't used by non-lazy deserialization
        System.arraycopy(buf, 0, dst, 0, offsets(0))
        var dstPos = offsets(0)
        offsetsAndLengths.foreach { case (o, l) =>
          System.arraycopy(buf, o, dst, dstPos, l)
          dstPos += l
        }
        dst
      }
    } else {
      // not just a mapping, but has actual functions/transforms - we have to evaluate the expressions
      val serializer = new KryoFeatureSerializer(transformSchema)
      val sf = new ScalaSimpleFeature("reusable", transformSchema)
      () => {
        sf.getIdentifier.setID(getID)
        var i = 0
        while (i < tdefs.size) {
          sf.setAttribute(i, tdefs(i).expression.evaluate(this))
          i += 1
        }
        serializer.serialize(sf)
      }
    }
  }

  override def getAttribute(index: Int) = {
    input.setPosition(offsets(index))
    readers(index)(input)
  }

  override def getType: SimpleFeatureType = sft
  override def getFeatureType: SimpleFeatureType = sft
  override def getName: Name = sft.getName

  override def getIdentifier: FeatureId = new FeatureIdImpl(getID)
  override def getID: String = {
    input.setPosition(5)
    input.readString()
  }

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
    val attributes = new java.util.ArrayList[AnyRef](offsets.length)
    var i = 0
    while (i < offsets.length) {
      attributes.add(getAttribute(i))
      i += 1
    }
    attributes
  }

  override def getUserData: jMap[AnyRef, AnyRef] = {
    if (userData == null) {
      userData = new jHashMap[AnyRef, AnyRef]()
    }
    userData
  }

  override def getDefaultGeometryProperty = ???
  override def getProperties: jCollection[Property] = ???
  override def getProperties(name: Name) = ???
  override def getProperties(name: String) = ???
  override def getProperty(name: Name) = ???
  override def getProperty(name: String) = ???
  override def getValue = ???
  override def getDescriptor = ???

  override def setAttribute(name: Name, value: Object) = ???
  override def setAttribute(name: String, value: Object) = ???
  override def setAttribute(index: Int, value: Object) = ???
  override def setAttributes(vals: jList[Object]) = ???
  override def setAttributes(vals: Array[Object]) = ???
  override def setDefaultGeometry(geo: Object) = ???
  override def setDefaultGeometryProperty(geoAttr: GeometryAttribute) = ???
  override def setValue(newValue: Object) = ???
  override def setValue(values: jCollection[Property]) = ???

  override def isNillable = true
  override def validate() = ???

  override def toString = s"KryoBufferSimpleFeature:$getID"
}
