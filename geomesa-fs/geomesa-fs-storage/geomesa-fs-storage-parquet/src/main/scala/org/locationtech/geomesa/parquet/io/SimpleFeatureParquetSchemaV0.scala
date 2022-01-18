/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.parquet.io

import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.{MessageType, OriginalType, Type, Types}
import org.locationtech.geomesa.utils.geotools.ObjectType
import org.locationtech.geomesa.utils.geotools.ObjectType.ObjectType
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.SimpleFeatureType

/**
  * Original parquet mapping - not versioned. Only supports points
  *
  * Of note, the FID field was marked as REPEATED, which seems to be an error and does not work with reading
  * parquet files as avro GenericRecords (which is the main way to read an unknown parquet file)
  */
object SimpleFeatureParquetSchemaV0 {

  import scala.collection.JavaConverters._

  def apply(sft: SimpleFeatureType): MessageType = {
    val idField =
      Types.primitive(PrimitiveTypeName.BINARY, Repetition.REPEATED)
          .as(OriginalType.UTF8)
          .named(SimpleFeatureParquetSchema.FeatureIdField)

    // NOTE: idField goes at the end of the record
    val fields = sft.getAttributeDescriptors.asScala.map(convertField) :+ idField
    new MessageType(sft.getTypeName, fields.asJava)
  }

  private def convertField(ad: AttributeDescriptor): Type = {
    val bindings = ObjectType.selectType(ad)
    val builder = bindings.head match {
      case ObjectType.GEOMETRY =>
        Types.buildGroup(Repetition.REQUIRED)
            .primitive(PrimitiveTypeName.DOUBLE, Repetition.REQUIRED).named(SimpleFeatureParquetSchema.GeometryColumnX)
            .primitive(PrimitiveTypeName.DOUBLE, Repetition.REQUIRED).named(SimpleFeatureParquetSchema.GeometryColumnY)

      case ObjectType.DATE    => Types.primitive(PrimitiveTypeName.INT64, Repetition.OPTIONAL)
      case ObjectType.STRING  => Types.primitive(PrimitiveTypeName.BINARY, Repetition.OPTIONAL).as(OriginalType.UTF8)
      case ObjectType.INT     => Types.primitive(PrimitiveTypeName.INT32, Repetition.OPTIONAL)
      case ObjectType.DOUBLE  => Types.primitive(PrimitiveTypeName.DOUBLE, Repetition.OPTIONAL)
      case ObjectType.LONG    => Types.primitive(PrimitiveTypeName.INT64, Repetition.OPTIONAL)
      case ObjectType.FLOAT   => Types.primitive(PrimitiveTypeName.FLOAT, Repetition.OPTIONAL)
      case ObjectType.BOOLEAN => Types.primitive(PrimitiveTypeName.BOOLEAN, Repetition.OPTIONAL)
      case ObjectType.BYTES   => Types.primitive(PrimitiveTypeName.BINARY, Repetition.OPTIONAL)
      case ObjectType.UUID    => Types.primitive(PrimitiveTypeName.BINARY, Repetition.OPTIONAL)
      case ObjectType.LIST    => Types.optionalList().optionalElement(matchType(bindings(1)))
      case ObjectType.MAP     => Types.optionalMap().key(matchType(bindings(1))).optionalValue(matchType(bindings(2)))
    }
    builder.named(ad.getLocalName)
  }

  private def matchType(objType: ObjectType): PrimitiveTypeName = {
    objType match {
      case ObjectType.DATE    => PrimitiveTypeName.INT64
      case ObjectType.STRING  => PrimitiveTypeName.BINARY
      case ObjectType.INT     => PrimitiveTypeName.INT32
      case ObjectType.DOUBLE  => PrimitiveTypeName.DOUBLE
      case ObjectType.LONG    => PrimitiveTypeName.INT64
      case ObjectType.FLOAT   => PrimitiveTypeName.FLOAT
      case ObjectType.BOOLEAN => PrimitiveTypeName.BOOLEAN
      case ObjectType.BYTES   => PrimitiveTypeName.BINARY
      case ObjectType.UUID    => PrimitiveTypeName.BINARY
    }
  }
}
