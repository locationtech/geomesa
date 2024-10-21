/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.fs.storage.parquet.io

import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema._
import org.locationtech.geomesa.features.serialization.TwkbSerialization.GeometryBytes
import org.locationtech.geomesa.fs.storage.parquet.io.SimpleFeatureParquetSchema.Binding
import org.locationtech.geomesa.utils.geotools.ObjectType
import org.locationtech.geomesa.utils.geotools.ObjectType.ObjectType
import org.locationtech.geomesa.utils.text.StringSerialization
import org.geotools.api.feature.`type`.AttributeDescriptor
import org.geotools.api.feature.simple.SimpleFeatureType

object SimpleFeatureParquetSchemaV1 {

  import org.locationtech.geomesa.fs.storage.parquet.io.SimpleFeatureParquetSchema.FeatureIdField

  import scala.collection.JavaConverters._

  val GeometryColumnX = "x"
  val GeometryColumnY = "y"

  /**
    * Get the message type for a simple feature type
    *
    * @param sft simple feature type
    * @return
    */
  def apply(sft: SimpleFeatureType): MessageType = {
    val id = Types.required(PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named(FeatureIdField)
    // note: id field goes at the end of the record
    val fields = sft.getAttributeDescriptors.asScala.map(schema) :+ id
    // ensure that we use a valid name - for avro conversion, especially, names are very limited
    new MessageType(StringSerialization.alphaNumericSafeString(sft.getTypeName), fields.asJava)
  }

  /**
    * Create a parquet field type from an attribute descriptor
    *
    * @param descriptor descriptor
    * @return
    */
  private def schema(descriptor: AttributeDescriptor): Type = {
    val bindings = ObjectType.selectType(descriptor)
    val builder = bindings.head match {
      case ObjectType.GEOMETRY => geometry(bindings(1))
      case ObjectType.LIST     => Binding(bindings(1)).list()
      case ObjectType.MAP      => Binding(bindings(1)).key(bindings(2))
      case p                   => Binding(p).primitive()
    }
    builder.named(StringSerialization.alphaNumericSafeString(descriptor.getLocalName))
  }

  /**
    * Create a builder for a parquet geometry field
    *
    * @param binding geometry type
    * @return
    */
  private def geometry(binding: ObjectType): Types.Builder[_, _ <: Type] = {
    def group: Types.GroupBuilder[GroupType] = Types.buildGroup(Repetition.OPTIONAL)
    binding match {
      case ObjectType.POINT =>
        group.id(GeometryBytes.TwkbPoint)
            .required(PrimitiveTypeName.DOUBLE).named(GeometryColumnX)
            .required(PrimitiveTypeName.DOUBLE).named(GeometryColumnY)

      case ObjectType.LINESTRING =>
        group.id(GeometryBytes.TwkbLineString)
            .repeated(PrimitiveTypeName.DOUBLE).named(GeometryColumnX)
            .repeated(PrimitiveTypeName.DOUBLE).named(GeometryColumnY)

      case ObjectType.MULTIPOINT =>
        group.id(GeometryBytes.TwkbMultiPoint)
            .repeated(PrimitiveTypeName.DOUBLE).named(GeometryColumnX)
            .repeated(PrimitiveTypeName.DOUBLE).named(GeometryColumnY)

      case ObjectType.POLYGON =>
        group.id(GeometryBytes.TwkbPolygon)
            .requiredList().element(PrimitiveTypeName.DOUBLE, Repetition.REPEATED).named(GeometryColumnX)
            .requiredList().element(PrimitiveTypeName.DOUBLE, Repetition.REPEATED).named(GeometryColumnY)

      case ObjectType.MULTILINESTRING =>
        group.id(GeometryBytes.TwkbMultiLineString)
            .requiredList().element(PrimitiveTypeName.DOUBLE, Repetition.REPEATED).named(GeometryColumnX)
            .requiredList().element(PrimitiveTypeName.DOUBLE, Repetition.REPEATED).named(GeometryColumnY)

      case ObjectType.MULTIPOLYGON =>
        group.id(GeometryBytes.TwkbMultiPolygon)
            .requiredList().requiredListElement().element(PrimitiveTypeName.DOUBLE, Repetition.REPEATED).named(GeometryColumnX)
            .requiredList().requiredListElement().element(PrimitiveTypeName.DOUBLE, Repetition.REPEATED).named(GeometryColumnY)

      case ObjectType.GEOMETRY =>
        Types.primitive(PrimitiveTypeName.BINARY, Repetition.OPTIONAL)

      case _ => throw new NotImplementedError(s"No mapping defined for geometry type $binding")
    }
  }
}
