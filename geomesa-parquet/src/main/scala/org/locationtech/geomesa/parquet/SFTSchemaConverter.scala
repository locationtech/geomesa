/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.parquet

import com.vividsolutions.jts.geom.Coordinate
import org.apache.parquet.io.api.{Binary, PrimitiveConverter}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.{MessageType, OriginalType, Type, Types}
import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.AttributeTypeBuilder
import org.geotools.feature.`type`.GeometryTypeImpl
import org.geotools.geometry.jts.JTSFactoryFinder
import org.locationtech.geomesa.features.serialization.ObjectType
import org.locationtech.geomesa.utils.geotools.SftBuilder
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
  * Created by afox on 5/25/17.
  */
object SFTSchemaConverter {


  def apply(sft: SimpleFeatureType): MessageType = {
    import scala.collection.JavaConversions._
    val idField =
      Types.primitive(PrimitiveTypeName.BINARY, Repetition.REQUIRED)
        .as(OriginalType.UTF8)
        .named("fid")

    // NOTE: idField goes at the end of the record
    new MessageType(sft.getTypeName, sft.getAttributeDescriptors.map(convertField) :+ idField)
  }

  def inverse(messageType: MessageType): SimpleFeatureType = {
    import scala.collection.JavaConversions._
    val sftBuilder = new SftBuilder
    val fields :+ idField = messageType.getFields.toList
    fields.foreach { c =>
      c.asPrimitiveType().getPrimitiveTypeName match {
        case PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY => sftBuilder.point(c.getName)
        case PrimitiveTypeName.INT64                => sftBuilder.date(c.getName) // TODO: need to use metadata to determine date
        case PrimitiveTypeName.BINARY               => sftBuilder.stringType(c.getName)
        case PrimitiveTypeName.INT32                => sftBuilder.intType(c.getName)
        case PrimitiveTypeName.INT64                => sftBuilder.longType(c.getName)
        case PrimitiveTypeName.DOUBLE               => sftBuilder.doubleType(c.getName)
        case _ => throw new RuntimeException("Implement me")
      }
    }
    sftBuilder.build(messageType.getName)
  }

  def convertField(ad: AttributeDescriptor): Type = {
    import PrimitiveTypeName._
    import Type.Repetition

    val binding = ad.getType.getBinding
    val (objectType, _) = ObjectType.selectType(binding, ad.getUserData)
    objectType match {
      case ObjectType.GEOMETRY =>
        // TODO: currently only dealing with Points packed into a 16 byte fixed array
        Types.primitive(FIXED_LEN_BYTE_ARRAY, Repetition.REQUIRED)
          .length(16)
          .named(ad.getLocalName)

      case ObjectType.DATE =>
        Types.primitive(INT64, Repetition.REQUIRED)
          .named(ad.getLocalName)

      case ObjectType.STRING =>
        Types.primitive(BINARY, Repetition.REQUIRED)
          .as(OriginalType.UTF8)
          .named(ad.getLocalName)

      case ObjectType.INT =>
        Types.primitive(INT32, Repetition.REQUIRED)
          .named(ad.getLocalName)

      case ObjectType.DOUBLE =>
        Types.primitive(DOUBLE, Repetition.REQUIRED)
          .named(ad.getLocalName)

      case ObjectType.LONG =>
        Types.primitive(INT64, Repetition.REQUIRED)
          .named(ad.getLocalName)

      case ObjectType.FLOAT =>
        Types.primitive(FLOAT, Repetition.REQUIRED)
          .named(ad.getLocalName)

      case ObjectType.BOOLEAN =>
        Types.primitive(BOOLEAN, Repetition.REQUIRED)
          .named(ad.getLocalName)

      case ObjectType.BYTES =>
        // TODO:
        null

      case ObjectType.LIST =>
        // TODO:
        null

      case ObjectType.MAP =>
        // TODO:
        null

      case ObjectType.UUID =>
        // TODO:
        null
    }
  }

  abstract class SimpleFeatureConverter(sf: SimpleFeature) extends PrimitiveConverter

  def converters(sft: SimpleFeatureType, sf: SimpleFeature): Array[SimpleFeatureConverter] = {
    import scala.collection.JavaConversions._
    sft.getAttributeDescriptors.zipWithIndex.map { case (ad, idx) => converterFor(ad, idx, sf) }.toArray
  }

  def converterFor(ad: AttributeDescriptor, index: Int, sf: SimpleFeature): SimpleFeatureConverter = {
    val binding = ad.getType.getBinding
    val (objectType, _) = ObjectType.selectType(binding, ad.getUserData)
    objectType match {
      case ObjectType.GEOMETRY =>
        new SimpleFeatureConverter(sf) {
          private val gf = JTSFactoryFinder.getGeometryFactory
          override def addBinary(value: Binary): Unit = {
            val buf = value.toByteBuffer
            val x = buf.getDouble()
            val y = buf.getDouble()
            val pt = gf.createPoint(new Coordinate(x, y))
            sf.setAttribute(index, pt)
          }
        }

      case ObjectType.DATE =>
        new SimpleFeatureConverter(sf)  {
          override def addLong(value: Long): Unit = {
            sf.setAttribute(index, value)
          }
        }

      case ObjectType.STRING =>
        new SimpleFeatureConverter(sf)  {
          override def addBinary(value: Binary): Unit = {
            sf.setAttribute(index, value.toStringUsingUTF8)
          }
        }

      case ObjectType.INT =>
        new SimpleFeatureConverter(sf)  {
          override def addInt(value: Int): Unit = {
            sf.setAttribute(index, value)
          }
        }

      case ObjectType.DOUBLE =>
        new SimpleFeatureConverter(sf) {
          override def addInt(value: Int): Unit = {
            sf.setAttribute(index, value.toDouble)
          }

          override def addDouble(value: Double): Unit = {
            sf.setAttribute(index, value)
          }

          override def addFloat(value: Float): Unit = {
            sf.setAttribute(index, value.toDouble)
          }

          override def addLong(value: Long): Unit = {
            sf.setAttribute(index, value.toDouble)
          }
        }

      case ObjectType.LONG =>
        new SimpleFeatureConverter(sf) {
          override def addLong(value: Long): Unit = {
            sf.setAttribute(index, value)
          }
        }


      case ObjectType.FLOAT =>
        new SimpleFeatureConverter(sf) {
          override def addFloat(value: Float): Unit = {
            sf.setAttribute(index, value)
          }
        }

      case ObjectType.BOOLEAN =>
        new SimpleFeatureConverter(sf) {
          override def addBoolean(value: Boolean): Unit = {
            sf.setAttribute(index, value)
          }
        }


      case ObjectType.BYTES =>
        new SimpleFeatureConverter(sf) {
          override def addBinary(value: Binary): Unit = {
            sf.setAttribute(index, value.getBytes)
          }
        }

      case ObjectType.LIST =>
        // TODO:
        null

      case ObjectType.MAP =>
        // TODO:
        null

      case ObjectType.UUID =>
        // TODO:
        null
    }

  }

  def buildAttributeWriters(sft: SimpleFeatureType): Array[AttributeWriter] = {
    import scala.collection.JavaConversions._
    sft.getAttributeDescriptors.zipWithIndex.map { case (ad, i) => AttributeWriter(ad, i) }.toArray
  }

}
