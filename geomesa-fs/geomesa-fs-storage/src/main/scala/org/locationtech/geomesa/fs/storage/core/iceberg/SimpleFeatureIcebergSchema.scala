/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.iceberg

import org.apache.iceberg.types.Types._
import org.apache.iceberg.{MetadataColumns, Schema}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.fs.storage.core.iceberg.SimpleFeatureIcebergSchema.IcebergFields
import org.locationtech.geomesa.fs.storage.core.parquet.schema.GeometrySchema.GeometryEncoding
import org.locationtech.geomesa.fs.storage.core.schema.SimpleFeatureSchema.{FeatureIdField, VisibilitiesField}
import org.locationtech.geomesa.fs.storage.core.schema.{BoundingBoxField, ColumnName, SimpleFeatureSchema, ZValueField}
import org.locationtech.geomesa.utils.geotools.ObjectType.ObjectType
import org.locationtech.geomesa.utils.geotools.Transform.{ExpressionTransform, PropertyTransform, RenameTransform, Transforms}
import org.locationtech.geomesa.utils.geotools.{ObjectType, SimpleFeatureTypes}
import org.locationtech.jts.geom.Geometry

import java.util.concurrent.atomic.AtomicInteger

/**
 * Holder for info about a geomesa/iceberg schema
 *
 * @param sft simple feature type represented by this schema
 * @param geometries geometry encoding
 * @param metadata table metadata
 * @param schema iceberg schema
 * @param fields field mappings, allowing for read transforms
 */
class SimpleFeatureIcebergSchema private (
    val sft: SimpleFeatureType,
    val geometries: GeometryEncoding,
    val metadata: java.util.Map[String, String],
    val schema: Schema,
    fields: IcebergFields) {

  import scala.collection.JavaConverters._

  /**
   * Gets the schema needed for reading a file
   *
   * @param transform query transform definition
   * @param filtered columns that have filters against them
   * @param includeRowPositions include _file and _pos columns necessary for handling updates/deletes
   * @return
   */
  def read(transform: Option[String], filtered: Set[String], includeRowPositions: Boolean = false): SimpleFeatureIcebergSchema = {
    val (readCols, readSft) = transform match {
      case None =>
        val baseCols =
          Seq(FeatureIdField, VisibilitiesField) ++ sft.getAttributeDescriptors.asScala.map(d => ColumnName.encode(d.getLocalName))
        val readCols = baseCols ++ filtered.filterNot(baseCols.contains)
        (readCols, sft)
      case Some(defs) =>
        val fromTransform = Transforms(sft, defs).flatMap {
          case t: PropertyTransform => Seq(sft.getDescriptor(t.i).getLocalName)
          case t: RenameTransform => Seq(sft.getDescriptor(t.i).getLocalName)
          case t: ExpressionTransform => FilterHelper.propertyNames(t.expression, sft)
          case t => throw new UnsupportedOperationException(s"An implementation is missing: ${t.getClass}")
        }
        val attributes = fromTransform.distinct
        val baseCols = Seq(FeatureIdField, VisibilitiesField) ++ attributes.map(ColumnName.encode)
        val readCols = baseCols ++ filtered.filterNot(baseCols.contains)
        val readSft = {
          val sftBuilder = new SimpleFeatureTypeBuilder()
          attributes.foreach(name => sftBuilder.add(sft.getDescriptor(name)))
          sftBuilder.setName(sft.getName)
          sftBuilder.buildFeatureType()
        }
        readSft.getUserData.putAll(sft.getUserData)
        (readCols, readSft)
    }
    val schema = fields.toSchema(readCols, includeRowPositions)
    new SimpleFeatureIcebergSchema(readSft, geometries, metadata, schema, fields)
  }
}

object SimpleFeatureIcebergSchema {

  import SimpleFeatureSchema._

  import scala.collection.JavaConverters._

  /**
   * Get a schema based on a simple feature type. Encoding can be configured through `geomesa.parquet.geometries`.
   *
   * @param conf write configuration, including the sft spec
   * @return
   */
  def apply(sft: SimpleFeatureType, conf: Map[String, String]): SimpleFeatureIcebergSchema = {
    val geometries = conf.get(GeometryEncodingKey).map(GeometryEncoding.apply).getOrElse(GeometryEncoding.GeoParquetWkb)
    val metadata = Map(
      SftNameKey -> sft.getTypeName,
      SftSpecKey -> SimpleFeatureTypes.encodeType(sft, includeUserData = true),
      GeometryEncodingKey -> geometries.toString,
    ) ++ conf.get(PartitionKey).map(PartitionKey -> _)
    val iceberg = schema(sft, geometries)
    new SimpleFeatureIcebergSchema(sft, geometries, metadata.asJava, iceberg.toSchema, iceberg)
  }

  private def schema(sft: SimpleFeatureType, geometries: GeometryEncoding): IcebergFields = {
    // note: for iceberg compatibility, field ids need to start at one and increment (without gaps) across all top-level fields.
    // All nested fields (structs, lists) get ids *after* all the top-level fields, once again incrementing without gaps
    val fieldIds = new AtomicInteger(1)
    val nestedFieldIds = {
      val extraGeomCount = 2 * sft.getAttributeDescriptors.asScala.count {
        case d if classOf[Geometry].isAssignableFrom(d.getType.getBinding) => true
        case _ => false
      }
      new AtomicInteger(3 + sft.getAttributeCount + extraGeomCount)
    }

    val builder = Seq.newBuilder[NestedField]
    builder += NestedField.required(fieldIds.getAndIncrement(), FeatureIdField, StringType.get())
    builder += NestedField.optional(fieldIds.getAndIncrement(), VisibilitiesField, StringType.get())

    sft.getAttributeDescriptors.asScala.foreach { d =>
      val name = ColumnName(d.getLocalName)
      val objectType = ObjectType.selectType(d)
      builder += buildField(name.column, objectType, geometries, fieldIds, nestedFieldIds)
      if (objectType.head == ObjectType.GEOMETRY) {
        builder += BoundingBoxField.icebergSchema(name.column, fieldIds, nestedFieldIds)
        builder += ZValueField.icebergSchema(name.column, objectType(1), fieldIds)
      }
    }

    val fields = builder.result()
    val aliases = fields.map(f => f.name() -> Int.box(f.fieldId())).toMap

    IcebergFields(fields, aliases)
  }

  /**
   * Builds the schema type for an attribute
   *
   * @param name field name
   * @param bindings object type
   * @param geometries geometry type encodings
   * @return
   */
  private def buildField(
      name: String,
      bindings: Seq[ObjectType],
      geometries: GeometryEncoding,
      fieldIds: AtomicInteger,
      nestedFieldIds: AtomicInteger): NestedField = {
    val builder = NestedField.optional(name).withId(fieldIds.getAndIncrement())
    val typed: NestedField.Builder = bindings.head match {
      case ObjectType.INT     => builder.ofType(IntegerType.get())
      case ObjectType.DOUBLE  => builder.ofType(DoubleType.get())
      case ObjectType.LONG    => builder.ofType(LongType.get())
      case ObjectType.FLOAT   => builder.ofType(FloatType.get())
      case ObjectType.BOOLEAN => builder.ofType(BooleanType.get())
      case ObjectType.BYTES   => builder.ofType(BinaryType.get())
      case ObjectType.STRING  => builder.ofType(StringType.get())
      case ObjectType.DATE    => builder.ofType(TimestampType.withZone())
      case ObjectType.UUID    => builder.ofType(UUIDType.get())

      case ObjectType.LIST =>
        val subType = buildField("", bindings.drop(1), geometries, nestedFieldIds, null /* should not be used*/)
        builder.ofType(ListType.ofRequired(subType.fieldId(), subType.`type`()))

      case ObjectType.MAP =>
        val keyType = buildField("", bindings.slice(1, 2), geometries, nestedFieldIds, null /* should not be used*/)
        val valueType = buildField("", bindings.slice(2, 3), geometries, nestedFieldIds, null /* should not be used*/)
        builder.ofType(MapType.ofRequired(keyType.fieldId(), valueType.fieldId(), keyType.`type`(), valueType.`type`()))

      case ObjectType.GEOMETRY =>
        // not yet supported in spark or trino
        // builder.ofType(GeometryType.crs84())
        builder.ofType(BinaryType.get())

      case binding =>
        throw new UnsupportedOperationException(s"No mapping defined for type: $binding")
    }
    typed.build()
  }

  private case class IcebergFields(fields: Seq[NestedField], aliases: Map[String, Integer]) {
    def toSchema: Schema = new Schema(fields.asJava, aliases.asJava, java.util.Set.of[Integer](1))
    def toSchema(cols: Seq[String], includeRowPositions: Boolean): Schema = {
      val projection =
        cols.map(col => fields.find(_.name() == col).getOrElse(throw new IllegalStateException(s"Unexpected projection: $col")))
      val withRows =
        if (includeRowPositions) { projection ++ Seq(MetadataColumns.FILE_PATH, MetadataColumns.ROW_POSITION) } else { projection }
      val ids = projection.collectFirst { case f if f.name() == FeatureIdField => Int.box(f.fieldId()) }
      new Schema(withRows.asJava, aliases.asJava, ids.toSet.asJava)
    }
  }
}
