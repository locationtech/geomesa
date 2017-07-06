/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
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
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.SimpleFeatureType

object SimpleFeatureParquetSchema {

  def apply(sft: SimpleFeatureType): MessageType = {
    import scala.collection.JavaConversions._
    val idField =
      Types.primitive(PrimitiveTypeName.BINARY, Repetition.REPEATED)
        .as(OriginalType.UTF8)
        .named("fid")

    // NOTE: idField goes at the end of the record
    new MessageType(sft.getTypeName, sft.getAttributeDescriptors.map(convertField) :+ idField)
  }

  def convertField(ad: AttributeDescriptor): Type = {
    import PrimitiveTypeName._
    import Type.Repetition

    val binding = ad.getType.getBinding
    val (objectType, _) = ObjectType.selectType(binding, ad.getUserData)
    objectType match {
      case ObjectType.GEOMETRY =>
        // TODO: currently only dealing with Points packed into a 16 byte fixed array
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

  def buildAttributeWriters(sft: SimpleFeatureType): Array[AttributeWriter] = {
    import scala.collection.JavaConversions._
    sft.getAttributeDescriptors.zipWithIndex.map { case (ad, i) => AttributeWriter(ad, i) }.toArray
  }

}
