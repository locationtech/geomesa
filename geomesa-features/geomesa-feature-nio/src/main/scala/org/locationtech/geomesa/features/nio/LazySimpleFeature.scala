/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.nio

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util
import java.util.Date

import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.{WKBReader, WKBWriter}
import org.geotools.feature.{AttributeImpl, GeometryAttributeImpl}
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.geometry.jts.ReferencedEnvelope
import org.opengis.feature.`type`.{AttributeDescriptor, GeometryDescriptor, Name}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.feature.{GeometryAttribute, Property}
import org.opengis.filter.identity.FeatureId
import org.opengis.geometry.BoundingBox

import scala.collection.JavaConversions._

sealed trait AttributeAccessor[T <: AnyRef] {
  def getAttribute(buf: ByteBuffer): T
}

object AttributeAccessor {
  def apply[T <: AnyRef](f: ByteBuffer => T): AttributeAccessor[T] =
    new AttributeAccessor[T] {
      override def getAttribute(buf: ByteBuffer): T = f(buf)
    }

  def readVariableLengthAttribute(buf: ByteBuffer, voffset: Int, offset: Int) = {
    val length = buf.getInt(voffset+offset)
    buf.position(voffset+offset+4)
    val ret = Array.ofDim[Byte](length)
    buf.get(ret, 0, length)
    ret
  }

  def descriptorLength(desc: AttributeDescriptor): Int = desc match {
    case _ if classOf[Integer].equals(desc.getType.getBinding)          => 4
    case _ if classOf[java.lang.Long].equals(desc.getType.getBinding)   => 8
    case _ if classOf[java.lang.Double].equals(desc.getType.getBinding) => 8
    case _ if classOf[java.util.Date].equals(desc.getType.getBinding)   => 8
    case _ => 4 // integer offset from start of variable section
  }

  private val wkbReader = new WKBReader()
  private val wkbWriter = new WKBWriter()
  def buildSimpleFeatureTypeAttributeAccessors(sft: SimpleFeatureType): IndexedSeq[AttributeAccessor[_ <: AnyRef]] = {
    val descriptors = sft.getAttributeDescriptors
    val voffset = descriptors.map(d => descriptorLength(d)).sum

    val (_, accessors) =
      descriptors.foldLeft((0, List.empty[AttributeAccessor[_ <: AnyRef]])) {
        case ((pos, acc), desc) if classOf[Integer].equals(desc.getType.getBinding) =>
          (pos+4, acc :+ AttributeAccessor[Integer](_.getInt(pos)))

        case ((pos, acc), desc) if classOf[java.lang.Long].equals(desc.getType.getBinding) =>
          (pos+8, acc :+ AttributeAccessor[java.lang.Long](_.getLong(pos)))

        case ((pos, acc), desc) if classOf[java.lang.Double].equals(desc.getType.getBinding) =>
          (pos+8, acc :+ AttributeAccessor[java.lang.Double](_.getDouble(pos)))

        case ((pos, acc), desc) if classOf[java.util.Date].equals(desc.getType.getBinding) =>
          (pos+8, acc :+ AttributeAccessor[java.util.Date] { buf =>
            val l = buf.getLong(pos)
            new Date(l)
          })

        case ((pos, acc), desc) if classOf[java.lang.Boolean].equals(desc.getType.getBinding) =>
          (pos+1, acc :+ AttributeAccessor[java.lang.Boolean] { buf =>
            val l = buf.get(pos)
            l != 0
          })

        case ((pos, acc), desc) if desc.isInstanceOf[GeometryDescriptor] =>
          (pos+4, acc :+ AttributeAccessor[Geometry] { buf =>
            val offset = buf.getInt(pos)
            val bytes = readVariableLengthAttribute(buf, voffset, offset)
            wkbReader.read(bytes)
          })

        case ((pos, acc), desc) if classOf[String].equals(desc.getType.getBinding) =>
          (pos+4, acc :+ AttributeAccessor[String] { buf =>
            val offset = buf.getInt(pos)
            val bytes = readVariableLengthAttribute(buf, voffset, offset)
            new String(bytes, StandardCharsets.UTF_8)
          })
      }
    accessors.toIndexedSeq
  }

  trait AttributeWriter[T <: AnyRef] {
    def write(attr: T, offset: Int, voffset: Int, buf: ByteBuffer): (Int, Int)
  }

  object AttributeWriter {
    def apply[T <: AnyRef](w: (T, Int, Int, ByteBuffer) => (Int, Int)) =
      new AttributeWriter[T] {
        override def write(attr: T, offset: Int, voffset: Int, buf: ByteBuffer): (Int, Int) =
          w(attr, offset, voffset, buf)
      }
  }

  class ByteBufferSimpleFeatureSerializer(sft: SimpleFeatureType) {
    val descriptors = sft.getAttributeDescriptors
    val vstart = descriptors.map(d => descriptorLength(d)).sum

    val attributeWriters =
      descriptors.map {
        case desc if classOf[Integer].equals(desc.getType.getBinding)          =>
          AttributeWriter[Integer] { (attr: Integer, offset: Int, voffset: Int, buf: ByteBuffer) =>
            buf.putInt(offset, attr)
            (4, 0)
          }

        case desc if classOf[java.lang.Long].equals(desc.getType.getBinding)   =>
          AttributeWriter[java.lang.Long] { (attr: java.lang.Long, offset: Int, voffset: Int, buf: ByteBuffer) =>
            buf.putLong(offset, attr)
            (8, 0)
          }

        case desc if classOf[java.lang.Double].equals(desc.getType.getBinding) =>
          AttributeWriter[java.lang.Double] { (attr: java.lang.Double, offset: Int, voffset: Int, buf: ByteBuffer) =>
            buf.putDouble(offset, attr)
            (8, 0)
          }

        case desc if classOf[java.util.Date].equals(desc.getType.getBinding)   =>
          AttributeWriter[java.util.Date] { (attr: java.util.Date, offset: Int, voffset: Int, buf: ByteBuffer) =>
            if (attr != null) {
              buf.putLong(offset, attr.getTime)
            }
            (8, 0)
          }

        case desc if classOf[java.lang.Boolean].equals(desc.getType.getBinding) =>
          AttributeWriter[java.lang.Boolean] { (attr: java.lang.Boolean, offset: Int, voffset: Int, buf: ByteBuffer) =>
            buf.put(offset, if(!attr) 0.toByte else 1.toByte)
            (1, 0)
          }

        case desc if desc.isInstanceOf[GeometryDescriptor] =>
          AttributeWriter[Geometry] { (attr: Geometry, offset: Int, voffset: Int, buf: ByteBuffer) =>
            buf.putInt(offset, voffset)
            val geom = wkbWriter.write(attr)
            val geomlength = geom.length
            buf.putInt(vstart+voffset, geomlength)
            val voffsetdatastart = vstart+voffset + 4
            buf.position(voffsetdatastart)
            buf.put(geom, 0, geomlength)
            (4, 4 + geomlength)
          }

        case desc if classOf[String].equals(desc.getType.getBinding) =>
          AttributeWriter[String] { (attr: String, offset: Int, voffset: Int, buf: ByteBuffer) =>
            buf.putInt(offset, voffset)
            val bytes = attr.getBytes(StandardCharsets.UTF_8)
            val blength = bytes.length
            buf.putInt(vstart + voffset, blength)
            val voffsetdatastart = vstart + voffset + 4
            buf.position(voffsetdatastart)
            buf.put(bytes, 0, blength)
            (4, 4+blength)
          }
      }.toIndexedSeq.asInstanceOf[IndexedSeq[AttributeWriter[AnyRef]]]

    def write(reuse: ByteBuffer, f: SimpleFeature): Int = {
      var curvoffset = 0
      var curoffset = 0
      val zipped = f.getAttributes.zip(attributeWriters)
      zipped.foreach { case (a, w) =>
        val (uoffset, uvoffset) = w.write(a.asInstanceOf[AnyRef], curoffset, curvoffset, reuse)
        curoffset += uoffset
        curvoffset += uvoffset
      }
      vstart + curvoffset
    }
  }
}

class LazySimpleFeature(id: String,
                        sft: SimpleFeatureType,
                        accessors: IndexedSeq[AttributeAccessor[_ <: AnyRef]],
                        var buf: ByteBuffer) extends SimpleFeature {

  def setBuf(reuse: ByteBuffer): Unit = {
    buf = reuse
  }

  override def getType: SimpleFeatureType = sft

  override def getID: String = id

  override def getAttributes: util.List[AnyRef] = List()

  override def getAttributeCount: Int = sft.getAttributeCount

  override def getAttribute(name: String): AnyRef = {
    val accessor = accessors(sft.indexOf(name))
    accessor.getAttribute(buf)
  }

  override def getAttribute(name: Name): AnyRef = getAttribute(name.getLocalPart)

  override def getAttribute(index: Int): AnyRef = accessors(index).getAttribute(buf)

  override def getDefaultGeometry: AnyRef = getAttribute(sft.getGeometryDescriptor.getName)

  override def setAttributes(values: util.List[AnyRef]): Unit = {}

  override def setAttributes(values: Array[AnyRef]): Unit = {}

  override def getFeatureType: SimpleFeatureType = sft

  override def setAttribute(name: String, value: scala.Any): Unit = {}

  override def setAttribute(name: Name, value: scala.Any): Unit = {}

  override def setAttribute(index: Int, value: scala.Any): Unit = {}

  override def setDefaultGeometry(geometry: scala.Any): Unit = {}

  override def setDefaultGeometryProperty(geometryAttribute: GeometryAttribute): Unit = {}

  override def getDefaultGeometryProperty: GeometryAttribute =
    new GeometryAttributeImpl(getDefaultGeometry, sft.getGeometryDescriptor, null)

  override def getIdentifier: FeatureId = new FeatureIdImpl(id)

  override def getBounds: BoundingBox =
    new ReferencedEnvelope(getDefaultGeometry.asInstanceOf[Geometry].getEnvelopeInternal, sft.getCoordinateReferenceSystem)

  override def getValue: util.Collection[_ <: Property] = getProperties

  override def setValue(values: util.Collection[Property]): Unit = {}

  override def getProperty(name: Name): Property =
    new AttributeImpl(getAttribute(name), sft.getDescriptor(name), getIdentifier)

  override def getProperty(name: String): Property =
    new AttributeImpl(getAttribute(name), sft.getDescriptor(name), getIdentifier)

  override def validate(): Unit = {}

  override def getProperties(name: Name): util.Collection[Property] = ???

  override def getProperties(name: String): util.Collection[Property] = ???

  override def getProperties: util.Collection[Property] = ???

  override def getDescriptor: AttributeDescriptor = ???

  override def isNillable: Boolean = ???

  override def setValue(newValue: scala.Any): Unit = ???

  override def getName: Name = sft.getName

  override def getUserData: util.Map[AnyRef, AnyRef] = Map.empty[AnyRef, AnyRef]
}
