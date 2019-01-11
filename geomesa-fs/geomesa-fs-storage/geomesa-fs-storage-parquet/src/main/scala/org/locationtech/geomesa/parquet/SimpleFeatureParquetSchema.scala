/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.parquet

import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.{MessageType, OriginalType, Type, Types}
import org.locationtech.geomesa.features.serialization.ObjectType
import org.locationtech.geomesa.features.serialization.ObjectType.ObjectType
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.SimpleFeatureType

object SimpleFeatureParquetSchema {

  val FeatureIDField = "__fid__"

  def apply(sft: SimpleFeatureType): MessageType = {
    import scala.collection.JavaConversions._
    val idField =
      Types.primitive(PrimitiveTypeName.BINARY, Repetition.REPEATED)
        .as(OriginalType.UTF8)
        .named(FeatureIDField)

    // NOTE: idField goes at the end of the record
    new MessageType(sft.getTypeName, sft.getAttributeDescriptors.map(convertField) :+ idField)
  }

  def convertField(ad: AttributeDescriptor): Type = {
    import PrimitiveTypeName._
    import Type.Repetition

    val bindings = ObjectType.selectType(ad)
    bindings.head match {
      case ObjectType.GEOMETRY =>
        // TODO: currently only dealing with Points
        Types.buildGroup(Repetition.REQUIRED)
          .primitive(DOUBLE, Repetition.REQUIRED).named("x")
          .primitive(DOUBLE, Repetition.REQUIRED).named("y")
          .named(ad.getLocalName)

      case ObjectType.DATE =>
        Types.primitive(INT64, Repetition.OPTIONAL)
          .named(ad.getLocalName)

      case ObjectType.STRING =>
        Types.primitive(BINARY, Repetition.OPTIONAL)
          .as(OriginalType.UTF8)
          .named(ad.getLocalName)

      case ObjectType.INT =>
        Types.primitive(INT32, Repetition.OPTIONAL)
          .named(ad.getLocalName)

      case ObjectType.DOUBLE =>
        Types.primitive(DOUBLE, Repetition.OPTIONAL)
          .named(ad.getLocalName)

      case ObjectType.LONG =>
        Types.primitive(INT64, Repetition.OPTIONAL)
          .named(ad.getLocalName)

      case ObjectType.FLOAT =>
        Types.primitive(FLOAT, Repetition.OPTIONAL)
          .named(ad.getLocalName)

      case ObjectType.BOOLEAN =>
        Types.primitive(BOOLEAN, Repetition.OPTIONAL)
          .named(ad.getLocalName)

      case ObjectType.BYTES =>
        Types.primitive(BINARY, Repetition.OPTIONAL)
          .named(ad.getLocalName)

      case ObjectType.LIST =>
        Types.optionalList().optionalElement(matchType(bindings(1)))
          .named(ad.getLocalName)

      case ObjectType.MAP =>
        Types.optionalMap()
          .key(matchType(bindings(1)))
          .optionalValue(matchType(bindings(2)))
          .named(ad.getLocalName)

      case ObjectType.UUID =>
        Types.primitive(BINARY, Repetition.OPTIONAL)
          .named(ad.getLocalName)
    }

  }

  private def matchType(objType: ObjectType): PrimitiveTypeName = {
    import PrimitiveTypeName._
    objType match {
      case ObjectType.DATE => INT64
      case ObjectType.STRING => BINARY
      case ObjectType.INT => INT32
      case ObjectType.DOUBLE => DOUBLE
      case ObjectType.LONG => INT64
      case ObjectType.FLOAT => FLOAT
      case ObjectType.BOOLEAN => BOOLEAN
      case ObjectType.BYTES => BINARY
      case ObjectType.UUID => BINARY
    }
  }

  def buildAttributeWriters(sft: SimpleFeatureType): Array[AttributeWriter] = {
    import scala.collection.JavaConversions._
    sft.getAttributeDescriptors.zipWithIndex.map { case (ad, i) => AttributeWriter(ad, i) }.toArray
  }

}
