/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.features.kryo

import java.util.{Collection => jCollection, List => jList, Map => jMap}

import com.esotericsoftware.kryo.io.Input
import com.vividsolutions.jts.geom.Geometry
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.process.vector.TransformProcess
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.serialization.ObjectType
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

class KryoBufferSimpleFeature(sft: SimpleFeatureType,
                              readers: Array[(Input) => AnyRef],
                              readUserData: (Input) => jMap[AnyRef, AnyRef]) extends SimpleFeature {

  private val input = new Input
  private val offsets = Array.ofDim[Int](sft.getAttributeCount)
  private var startOfOffsets: Int = -1
  private lazy val geomIndex = sft.indexOf(sft.getGeometryDescriptor.getLocalName)
  private var userData: jMap[AnyRef, AnyRef] = null
  private var userDataOffset: Int = -1

  private var binaryTransform: () => Array[Byte] = input.getBuffer
  private var reserializeTransform: () => Array[Byte] = input.getBuffer

  def transform(): Array[Byte] = if (offsets.contains(-1)) reserializeTransform() else binaryTransform()

  def setBuffer(bytes: Array[Byte]) = {
    input.setBuffer(bytes)
    // reset our offsets
    input.setPosition(1) // skip version
    startOfOffsets = input.readInt()
    input.setPosition(startOfOffsets) // set to offsets start
    var i = 0
    while (i < offsets.length) {
      offsets(i) = if (input.position < input.limit) input.readInt(true) else -1
      i += 1
    }
    userData = null
    userDataOffset = input.position()
  }

  def setTransforms(transforms: String, transformSchema: SimpleFeatureType) = {
    val tdefs = TransformProcess.toDefinition(transforms)

    // transforms by evaluating the transform expressions and then serializing the resulting feature
    // we use this for transform expressions and for data that was written using an old schema
    reserializeTransform = {
      val serializer = new KryoFeatureSerializer(transformSchema)
      val sf = new ScalaSimpleFeature("", transformSchema)
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

    // if we are just returning a subset of attributes, we can copy the bytes directly and avoid creating
    // new objects, reserializing, etc
    val isSimpleMapping = tdefs.forall(_.expression.isInstanceOf[PropertyName])
    binaryTransform = if (isSimpleMapping) {
      val indices = tdefs.map(t => sft.indexOf(t.expression.asInstanceOf[PropertyName].getPropertyName))
      () => {
        val buf = input.getBuffer
        var length = offsets(0) // space for version, offset block and ID
        val offsetsAndLengths = indices.map { i =>
          val l = (if (i < offsets.length - 1) offsets(i + 1) else startOfOffsets) - offsets(i)
          length += l
          (offsets(i), l)
        }
        val dst = Array.ofDim[Byte](length)
        // copy the version, offset block and id
        var dstPos = offsets(0)
        System.arraycopy(buf, 0, dst, 0, dstPos)
        offsetsAndLengths.foreach { case (o, l) =>
          System.arraycopy(buf, o, dst, dstPos, l)
          dstPos += l
        }
        // note that the offset block is incorrect - we couldn't use this in another lazy feature
        // but the normal serializer doesn't care
        dst
      }
    } else {
      reserializeTransform
    }
  }

  def getDateAsLong(index: Int): Long = {
    val offset = offsets(index)
    if (offset == -1) {
      0L
    } else {
      input.setPosition(offset)
      KryoBufferSimpleFeature.longReader(input).asInstanceOf[Long]
    }
  }

  override def getAttribute(index: Int) = {
    val offset = offsets(index)
    if (offset == -1) {
      null
    } else {
      input.setPosition(offset)
      readers(index)(input)
    }
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
      input.setPosition(userDataOffset)
      userData = readUserData(input)
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

object KryoBufferSimpleFeature {
  val longReader = KryoFeatureSerializer.matchReader(ObjectType.LONG)
}