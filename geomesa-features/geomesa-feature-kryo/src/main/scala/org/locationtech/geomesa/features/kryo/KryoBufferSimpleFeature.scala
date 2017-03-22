/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.kryo

import java.util.{Collection => jCollection, List => jList, Map => jMap}

import com.esotericsoftware.kryo.io.Input
import com.vividsolutions.jts.geom.Geometry
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.process.vector.TransformProcess
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.SerializationOption._
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
                              readUserData: (Input) => jMap[AnyRef, AnyRef],
                              options: Set[SerializationOption]) extends SimpleFeature {

  private val input = new Input
  private val offsets = Array.ofDim[Int](sft.getAttributeCount)
  private var startOfOffsets: Int = -1
  private var missingAttributes: Boolean = false
  private lazy val geomIndex = sft.indexOf(sft.getGeometryDescriptor.getLocalName)
  private var userData: jMap[AnyRef, AnyRef] = null
  private var userDataOffset: Int = -1

  private var id: String = ""

  private var transforms: String = null
  private var transformSchema: SimpleFeatureType = null
  private var binaryTransform: () => Array[Byte] = input.getBuffer
  private var reserializeTransform: () => Array[Byte] = input.getBuffer

  /**
    * Creates a new feature for later use - does not copy attribute bytes
    *
    * @return
    */
  def copy(): KryoBufferSimpleFeature = {
    val sf = new KryoBufferSimpleFeature(sft, readers, readUserData, options)
    if (transforms != null) {
      sf.setTransforms(transforms, transformSchema)
    }
    sf
  }

  /**
    * Transform the feature into a serialized byte array
    *
    * @return
    */
  def transform(): Array[Byte] =
    // if attributes have been added to the sft, we have to reserialize to get the null serialized values
    if (missingAttributes) { reserializeTransform() } else { binaryTransform() }

  /**
    * Set the serialized bytes to use for reading attributes
    *
    * @param bytes serialized byte array
    */
  def setBuffer(bytes: Array[Byte]): Unit = setBuffer(bytes, 0, bytes.length)

  /**
    * Set the serialized bytes to use for reading attributes
    *
    * @param bytes serialized byte array
    * @param offset offset into the byte array of valid bytes
    * @param length number of valid bytes to read from the byte array
    */
  def setBuffer(bytes: Array[Byte], offset: Int, length: Int): Unit = {
    input.setBuffer(bytes, offset, offset + length)
    // reset our offsets
    input.setPosition(offset + 1) // skip version
    startOfOffsets = input.readInt()
    input.setPosition(offset + startOfOffsets) // set to offsets start
    var i = 0
    while (i < offsets.length && input.position < input.limit) {
      offsets(i) = offset + input.readInt(true)
      i += 1
    }
    if (i < offsets.length) {
      // attributes have been added to the sft since this feature was serialized
      missingAttributes = true
      do { offsets(i) = -1; i += 1 } while (i < offsets.length)
    } else {
      missingAttributes = false
    }
    userData = null
    userDataOffset = input.position()
  }

  def setId(id: String): Unit = this.id = id

  def setTransforms(transforms: String, transformSchema: SimpleFeatureType) = {
    this.transforms = transforms
    this.transformSchema = transformSchema

    val tdefs = TransformProcess.toDefinition(transforms)

    // transforms by evaluating the transform expressions and then serializing the resulting feature
    // we use this for transform expressions and for data that was written using an old schema
    reserializeTransform = {
      val serializer = new KryoFeatureSerializer(transformSchema, options)
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

    val indices = tdefs.map { t =>
      t.expression match {
        case p: PropertyName => sft.indexOf(p.getPropertyName)
        case _ => -1
      }
    }

    val shouldReserialize = indices.contains(-1)

    // if we are just returning a subset of attributes, we can copy the bytes directly and avoid creating
    // new objects, reserializing, etc
    binaryTransform = if (!shouldReserialize) {
      val mutableOffsetsAndLength = Array.ofDim[(Int,Int)](indices.length)

      () => {
        val buf = input.getBuffer
        var length = offsets(0) // space for version, offset block and ID
        var idx = 0
        while(idx < mutableOffsetsAndLength.length) {
          val i = indices(idx)
          val l = (if (i < offsets.length - 1) offsets(i + 1) else startOfOffsets) - offsets(i)
          length += l
          mutableOffsetsAndLength(idx) = (offsets(i), l)
          idx += 1
        }

        val dst = Array.ofDim[Byte](length)
        // copy the version, offset block and id
        var dstPos = offsets(0)
        System.arraycopy(buf, 0, dst, 0, dstPos)
        mutableOffsetsAndLength.foreach { case (o, l) =>
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

  override def getAttribute(index: Int): AnyRef = {
    val offset = offsets(index)
    if (offset == -1) {
      null
    } else {
      input.setPosition(offset)
      readers(index)(input)
    }
  }

  def getInput(index: Int): Input = {
    val offset = offsets(index)
    if (offset == -1) {
      null
    } else {
      input.setPosition(offset)
      input
    }
  }

  override def getType: SimpleFeatureType = sft
  override def getFeatureType: SimpleFeatureType = sft
  override def getName: Name = sft.getName

  override def getIdentifier: FeatureId = new FeatureIdImpl(getID)
  override def getID: String = {
    if (options.withoutId) { id } else {
      input.setPosition(5)
      input.readString()
    }
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

  override def toString = s"KryoBufferSimpleFeature:$getID"
}

object KryoBufferSimpleFeature {
  val longReader = KryoFeatureSerializer.matchReader(ObjectType.LONG)
}