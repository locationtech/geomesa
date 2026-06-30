/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.parquet.schema

import com.typesafe.scalalogging.LazyLogging
import org.apache.parquet.conf.{ParquetConfiguration, PlainParquetConfiguration}
import org.apache.parquet.hadoop.api.InitContext
import org.apache.parquet.hadoop.metadata.FileMetaData
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.{LogicalTypeAnnotation, MessageType, Type, Types}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.fs.storage.core.parquet.schema.GeometrySchema.GeometryEncoding
import org.locationtech.geomesa.fs.storage.core.parquet.schema.SimpleFeatureParquetSchema.ParquetFields
import org.locationtech.geomesa.fs.storage.core.schema.{BoundingBoxField, ColumnName, SimpleFeatureSchema, ZValueField}
import org.locationtech.geomesa.utils.geotools.ObjectType.ObjectType
import org.locationtech.geomesa.utils.geotools.{ObjectType, SimpleFeatureTypes}
import org.locationtech.jts.geom.Geometry

import java.util.concurrent.atomic.AtomicInteger

/**
 * A paired simple feature type and parquet schema
 *
 * @param sft simple feature type
 * @param geometries geometry type encoding
 * @param metadata file metadata
 * @param messageType parquet schema
 * @param fields parquet message schema
 */
class SimpleFeatureParquetSchema private (
    val sft: SimpleFeatureType,
    val geometries: GeometryEncoding,
    val metadata: java.util.Map[String, String],
    val messageType: MessageType,
    fields: ParquetFields) {

  /**
   * Gets the schema used for reading (excludes some columns only used for partitioning)
   *
   * @return
   */
  def read(): SimpleFeatureParquetSchema = {
    val fieldFilter =
      fields.fields.map(_.getName)
        .filterNot(n => n.endsWith(ZValueField.Z2ValueFieldSuffix) || n.endsWith(ZValueField.XZ2ValueFieldSuffix))
    new SimpleFeatureParquetSchema(sft, geometries, metadata, fields.toMessageType(sft.getTypeName, fieldFilter.toSet), fields)
  }
}

object SimpleFeatureParquetSchema extends LazyLogging {

  import SimpleFeatureSchema._

  import scala.collection.JavaConverters._

  /**
   * Encodes the feature type in the conf
   *
   * @param conf conf
   * @param sft feature type
   */
  def setSft(conf: ParquetConfiguration, sft: SimpleFeatureType): Unit = {
    val name = Option(sft.getName.getNamespaceURI).map(ns => s"$ns:${sft.getTypeName}").getOrElse(sft.getTypeName)
    conf.set(SftNameKey, name)
    conf.set(SftSpecKey, SimpleFeatureTypes.encodeType(sft, includeUserData = true))
  }

  /**
    * Extract the simple feature type from a parquet read context. The read context
    * contains both file metadata and the provided read conf
    *
    * @param context parquet read context
    * @return
    */
  def read(context: InitContext): Option[SimpleFeatureParquetSchema] = {
    // copy in the file level metadata
    val kvMeta = context.getKeyValueMetadata.asScala.collect { case (k, v) if !v.isEmpty => k -> v.iterator.next }
    // noinspection ScalaDeprecation
    val gmMeta = Seq(SftNameKey, SftSpecKey, GeometryEncodingKey).flatMap { key =>
      val value =
        try { context.getParquetConfiguration.get(key) } catch {
          // noinspection ScalaDeprecation - use the older method for spark compatibility
          case _: NoSuchMethodError => context.getConfiguration.get(key)
        }
      Option(value).map(key -> _)
    }
    // note: context meta goes second so that transforms override the file-level keys
    apply(new PlainParquetConfiguration((kvMeta.toMap ++ gmMeta.toMap).asJava)).map(_.read())
  }

  /**
    * Extract the simple feature type from an existing parquet file, without any known read context
    *
    * @param footer parquet file footer
    * @return
    */
  def read(footer: FileMetaData): Option[SimpleFeatureParquetSchema] =
    apply(new PlainParquetConfiguration(footer.getKeyValueMetaData)).map(_.read())

  /**
   * Get a schema for writing. Encoding can be configured through `geomesa.parquet.geometries` and `geomesa.fs.visibilities`
   *
   * @param conf write configuration, including the sft spec
   * @return
   */
  def apply(conf: ParquetConfiguration): Option[SimpleFeatureParquetSchema] = {
    for {
      name <- Option(conf.get(SftNameKey))
      spec <- Option(conf.get(SftSpecKey))
    } yield {
      val sft = SimpleFeatureTypes.createImmutableType(name, spec)
      val geometries = Option(conf.get(GeometryEncodingKey)).map(GeometryEncoding.apply).getOrElse(GeometryEncoding.GeoParquetWkb)
      val metadata = Map(
        SftNameKey -> name,
        SftSpecKey -> spec,
        GeometryEncodingKey -> geometries.toString,
      ) ++ Option(conf.get(PartitionKey)).map(PartitionKey -> _)
      val parquet = schema(sft, geometries)
      new SimpleFeatureParquetSchema(sft, geometries, metadata.asJava, parquet.toMessageType(sft.getTypeName), parquet)
    }
  }

  /**
   * Gets the parquet schema for a feature type
   *
   * @param sft simple feature type
   * @param conf storage configuration
   * @return
   */
  def apply(sft: SimpleFeatureType, conf: Map[String, String]): SimpleFeatureParquetSchema = {
    val config = new PlainParquetConfiguration(conf.asJava)
    setSft(config, sft)
    apply(config).get
  }

  /**
   * Get the message type for a simple feature type. We need the full sft in order to ensure field ids are
   * consistent, but the schema may be reduced (e.g. on read) based on the filter sft
   *
   * @param sft simple feature type
   * @param geometries geometry type encoding
   * @return
   */
  private def schema(sft: SimpleFeatureType, geometries: GeometryEncoding): ParquetFields = {
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

    val builder = Seq.newBuilder[Type]
    builder +=
      Types.required(PrimitiveTypeName.BINARY)
        .id(fieldIds.getAndIncrement())
        .as(LogicalTypeAnnotation.stringType())
        .named(FeatureIdField)
    builder +=
      Types.optional(PrimitiveTypeName.BINARY)
        .id(fieldIds.getAndIncrement())
        .as(LogicalTypeAnnotation.stringType())
        .named(VisibilitiesField)

    sft.getAttributeDescriptors.asScala.foreach { d =>
      val name = ColumnName(d.getLocalName)
      val objectType = ObjectType.selectType(d)
      builder += buildType(name.column, objectType, geometries, fieldIds, nestedFieldIds)
      if (objectType.head == ObjectType.GEOMETRY) {
        builder += BoundingBoxField.parquetSchema(name.column, fieldIds, nestedFieldIds)
        builder += ZValueField.parquetSchema(name.column, objectType(1), fieldIds)
      }
    }
    ParquetFields(builder.result())
  }

  /**
   * Builds the schema type for an attribute
   *
   * @param name field name
   * @param bindings object type
   * @param geometries geometry type encodings
   * @param repetition repetition
   * @return
   */
  private def buildType(
      name: String,
      bindings: Seq[ObjectType],
      geometries: GeometryEncoding,
      fieldIds: AtomicInteger,
      nestedFieldIds: AtomicInteger,
      repetition: Repetition = Repetition.OPTIONAL): Type = {
    val builder = bindings.head match {
      case ObjectType.INT     => Types.primitive(PrimitiveTypeName.INT32, repetition).id(fieldIds.getAndIncrement())
      case ObjectType.DOUBLE  => Types.primitive(PrimitiveTypeName.DOUBLE, repetition).id(fieldIds.getAndIncrement())
      case ObjectType.LONG    => Types.primitive(PrimitiveTypeName.INT64, repetition).id(fieldIds.getAndIncrement())
      case ObjectType.FLOAT   => Types.primitive(PrimitiveTypeName.FLOAT, repetition).id(fieldIds.getAndIncrement())
      case ObjectType.BOOLEAN => Types.primitive(PrimitiveTypeName.BOOLEAN, repetition).id(fieldIds.getAndIncrement())
      case ObjectType.BYTES   => Types.primitive(PrimitiveTypeName.BINARY, repetition).id(fieldIds.getAndIncrement())

      case ObjectType.STRING =>
        Types.primitive(PrimitiveTypeName.BINARY, repetition)
          .id(fieldIds.getAndIncrement())
          .as(LogicalTypeAnnotation.stringType())

      case ObjectType.DATE =>
        Types.primitive(PrimitiveTypeName.INT64, repetition)
          .id(fieldIds.getAndIncrement())
          .as(LogicalTypeAnnotation.timestampType(true, TimeUnit.MICROS))

      case ObjectType.UUID =>
        Types.primitive(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, repetition)
          .id(fieldIds.getAndIncrement())
          .length(16)
          .as(LogicalTypeAnnotation.uuidType())

      case ObjectType.LIST =>
        Types.optionalList()
          .id(fieldIds.getAndIncrement())
          .element(buildType("element", bindings.drop(1), geometries, nestedFieldIds, null /* should not be used*/, Repetition.REQUIRED))

      case ObjectType.MAP =>
        Types.optionalMap()
          .id(fieldIds.getAndIncrement())
          .key(buildType("key", bindings.slice(1, 2), geometries, nestedFieldIds, null /* should not be used*/, Repetition.REQUIRED))
          .value(buildType("value", bindings.slice(2, 3), geometries, nestedFieldIds, null /* should not be used*/))

      case ObjectType.GEOMETRY =>
        geometries.schema(bindings(1), fieldIds, nestedFieldIds)

      case binding =>
        throw new UnsupportedOperationException(s"No mapping defined for type: $binding")
    }
    builder.named(name)
  }


  private case class ParquetFields(fields: Seq[Type]) {
    def toMessageType(typeName: String): MessageType = new MessageType(ColumnName.encode(typeName), fields.asJava)
    def toMessageType(typeName: String, fieldFilter: Set[String]): MessageType =
      new MessageType(ColumnName.encode(typeName), fields.filter(f => fieldFilter.contains(f.getName)).asJava)
  }
}
