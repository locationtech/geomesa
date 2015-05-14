/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.feature.kryo

import java.util.{Collection => jCollection, List => jList}

import com.esotericsoftware.kryo.io.Input
import com.vividsolutions.jts.geom.Geometry
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.process.vector.TransformProcess
import org.locationtech.geomesa.feature.FeatureEncoding._
import org.locationtech.geomesa.feature.{FeatureEncoding, ScalaSimpleFeature, SimpleFeatureEncoder}
import org.locationtech.geomesa.feature.serialization.{DatumReader, DecodingsVersionCache, KryoSerialization}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.feature.{GeometryAttribute, Property}
import org.opengis.filter.expression.PropertyName
import org.opengis.geometry.BoundingBox
import scala.collection.JavaConversions._

object LazySimpleFeature {
  val NULL_BYTE = 0.asInstanceOf[Byte]
}

class KryoBufferSimpleFeature(sft: SimpleFeatureType, readers: List[(Input) => AnyRef], transforms: Option[String] = None) extends SimpleFeature {

  private val input = new Input
  private val offsets = Array.ofDim[Int](sft.getAttributeCount)

  private var id: String = null

//  private lazy val binaryTransform: () => Array[Byte] = transforms match {
//    case None => () => input.getBuffer
//    case Some(t) =>
//      val tdefs = TransformProcess.toDefinition(t)
//      if (tdefs.forall(_.expression.isInstanceOf[PropertyName])) {
//        val indices = tdefs.map(d => sft.indexOf(d.expression.asInstanceOf[PropertyName].getPropertyName))
//        val maxIndex = indices.max
//        val last = maxIndex == offsets.length - 1
//        () => {
//          val buf = input.getBuffer
//          if (last) { ensureOffsets(maxIndex) } else { ensureOffsets(maxIndex + 1) }
//          var length = 0
//          val offsetsAndLengths = indices.map { i =>
//            val l = (if (i < offsets.length - 1) offsets(i + 1) else buf.length) - offsets(i)
//            length += l
//            (offsets(i), l)
//          }
//          val dst = Array.ofDim[Byte](length)
//          var dstPos = 0
//          offsetsAndLengths.foreach { case (o, l) =>
//            System.arraycopy(buf, o, dst, dstPos, l)
//            dstPos += l
//          }
//          dst
//        }
//      } else {
//        // not just a mapping, but has actual functions/transforms
//        val spec = tdefs.map(t => s"${t.name}:${t.binding}").mkString(",")
//        val targetFeatureType = SimpleFeatureTypes.createType("t", spec)
//        val encoder = SimpleFeatureEncoder(targetFeatureType, FeatureEncoding.KRYO)
//        val sf = new ScalaSimpleFeature("reusable", targetFeatureType)
//        () => {
//          sf.getIdentifier.setID(getID)
//          tdefs.foreach(t => sf.setAttribute(t.name, t.expression.evaluate(this)))
//          encoder.encode(sf)
//        }
//      }
//  }

  lazy val geomIndex = sft.indexOf(sft.getGeometryDescriptor.getLocalName)

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
  }

  override def getAttribute(index: Int) = {
    input.setPosition(offsets(index))
    readers(index)(input)
  }

//  def transform(): Array[Byte] = binaryTransform()

  override def getFeatureType = sft
  override def getType = sft
  override def getIdentifier = new FeatureIdImpl(getID)
  override def getID = {
    // TODO this needs to reference the featureId, as it can be updated
    input.setPosition(5)
    input.readString()
  }
  override def getName = sft.getName

  override def getAttribute(name: Name) = getAttribute(name.getLocalPart)
  override def getAttribute(name: String) = {
    val index = sft.indexOf(name)
    if (index == -1) null else getAttribute(index)
  }

  override def getDefaultGeometry: Object = if (geomIndex == -1) null else getAttribute(geomIndex)
  override def getAttributeCount = sft.getAttributeCount

  override def getBounds: BoundingBox = getDefaultGeometry match {
    case g: Geometry => new ReferencedEnvelope(g.getEnvelopeInternal, sft.getCoordinateReferenceSystem)
    case _ => new ReferencedEnvelope(sft.getCoordinateReferenceSystem)
  }

  override def setAttribute(name: Name, value: Object) = ???
  override def setAttribute(name: String, value: Object) = ???
  override def setAttribute(index: Int, value: Object) = ???
  override def setAttributes(vals: jList[Object]) = ???
  override def setAttributes(vals: Array[Object]) = ???
  override def getAttributes: jList[Object] = ???
  override def setDefaultGeometry(geo: Object) = ???
  override def getDefaultGeometryProperty = ???
  override def setDefaultGeometryProperty(geoAttr: GeometryAttribute) = ???
  override def getProperties: jCollection[Property] = ???
  override def getProperties(name: Name) = ???
  override def getProperties(name: String) = ???
  override def getProperty(name: Name) = ???
  override def getProperty(name: String) = ???
  override def getUserData = ???
  override def getValue = ???
  override def setValue(newValue: Object) = ???
  override def setValue(values: jCollection[Property]) = ???
  override def getDescriptor = ???
  override def isNillable = true
  override def validate() = ???

  override def toString = s"LazyKryoSimpleFeature:$getID"
}
