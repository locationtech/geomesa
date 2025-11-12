/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/


package org.locationtech.geomesa.fs.storage.parquet.io

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.conf.ParquetConfiguration
import org.apache.parquet.hadoop.api.InitContext
import org.apache.parquet.hadoop.metadata.FileMetaData
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema._
import org.geotools.api.feature.`type`.AttributeDescriptor
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.fs.storage.parquet.io.GeometrySchema.{BoundingBoxField, GeometryEncoding}
import org.locationtech.geomesa.utils.geotools.ObjectType.ObjectType
import org.locationtech.geomesa.utils.geotools.{ObjectType, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.text.StringSerialization

import java.util.Collections

/**
 * A paired simple feature type and parquet schema
 *
 * @param sft simple feature type
 * @param encodings type encoding
 * @param hasVisibilities whether the schema encodes visibilities, or not
 * @param boundingBoxes names of any bounding box fields, along with the original field name
 * @param metadata file metadata
 */
case class SimpleFeatureParquetSchema(
    sft: SimpleFeatureType,
    encodings: Encodings,
    hasVisibilities: Boolean,
    boundingBoxes: Seq[BoundingBoxField],
    metadata: java.util.Map[String, String]) {

  /**
   * Parquet message schema
   */
  val schema: MessageType = SimpleFeatureParquetSchema.schema(sft, encodings, boundingBoxes.map(_.bbox), hasVisibilities)

  /**
    * Gets the name of the parquet field for the given simple feature type attribute
    *
    * @param i index of the sft attribute
    * @return
    */
  def field(i: Int): String = schema.getFields.get(i).getName
}

object SimpleFeatureParquetSchema extends LazyLogging {

  import StringSerialization.alphaNumericSafeString

  import scala.collection.JavaConverters._

  val FeatureIdField    = "__fid__"
  val VisibilitiesField = "__vis__"

  /**
   * Schema versions:
   *   * 0/not present - Deprecated schema that only supports points, incorrectly marks feature ID as 'repeated', and doesn't safely encode names
   *   * 1 - GeoMesa-specific encoding that supports all geometries, compatible with GeoParquet for points only
   */
  @deprecated("Track schema options separately with `GeometryEncodingKey` and `VisibilityEncodingKey`")
  val SchemaVersionKey = "geomesa.parquet.version"

  val SftNameKey            = "geomesa.fs.sft.name"
  val SftSpecKey            = "geomesa.fs.sft.spec"
  val GeometryEncodingKey   = "geomesa.parquet.geometries"
  val BBoxEncodingKey       = "geomesa.parquet.bounding-boxes"
  val VisibilityEncodingKey = "geomesa.fs.visibilities"

  @deprecated("Moved to org.locationtech.geomesa.fs.storage.parquet.io.GeometrySchema")
  val GeometryColumnX: String = GeometrySchema.GeometryColumnX
  @deprecated("Moved to org.locationtech.geomesa.fs.storage.parquet.io.GeometrySchema")
  val GeometryColumnY: String = GeometrySchema.GeometryColumnY

  /**
   * Encodes the feature type in the conf
   *
   * @param conf conf
   * @param sft feature type
   */
  def setSft(conf: Configuration, sft: SimpleFeatureType): Unit = {
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
    val metadata = new java.util.HashMap[String, String]()
    // copy in the file level metadata - note, do this before copying in the conf so that transform schemas are applied correctly
    context.getKeyValueMetadata.asScala.foreach { case (k, v) => if (!v.isEmpty) { metadata.put(k, v.iterator.next) }}
    // noinspection ScalaDeprecation - use the older method for spark compatibility
    val conf = context.getConfiguration
    // noinspection ScalaDeprecation
    Seq(SftNameKey, SftSpecKey, GeometryEncodingKey, SchemaVersionKey).foreach { key =>
      Option(conf.get(key)).foreach(metadata.put(key, _))
    }
    read(context.getFileSchema, metadata)
  }

  /**
    * Extract the simple feature type from an existing parquet file, without any known read context
    *
    * @param footer parquet file footer
    * @return
    */
  def read(footer: FileMetaData): Option[SimpleFeatureParquetSchema] = read(footer.getSchema, footer.getKeyValueMetaData)

  /**
   * Get a schema for writing. Encoding can be configured through `geomesa.parquet.geometries` and `geomesa.fs.visibilities`
   *
   * @param conf write configuration, including the sft spec
   * @return
   */
  def write(conf: Configuration): Option[SimpleFeatureParquetSchema] = {
    val vis = conf.get(VisibilityEncodingKey)
    write(conf.get(SftNameKey), conf.get(SftSpecKey), conf.get(GeometryEncodingKey), conf.get(BBoxEncodingKey), vis)
  }

  /**
   * Get a schema for writing. Encoding can be configured through `geomesa.parquet.geometries` and `geomesa.fs.visibilities`
   *
   * @param conf write configuration, including the sft spec
   * @return
   */
  def write(conf: ParquetConfiguration): Option[SimpleFeatureParquetSchema] = {
    val vis = conf.get(VisibilityEncodingKey)
    write(conf.get(SftNameKey), conf.get(SftSpecKey), conf.get(GeometryEncodingKey), conf.get(BBoxEncodingKey), vis)
  }

  /**
   * Get the schema for a file being read
   *
   * @param fileSchema message type
   * @param metadata file metadata
   * @return
   */
  private def read(fileSchema: MessageType, metadata: java.util.Map[String, String]): Option[SimpleFeatureParquetSchema] = {
    for {
      name <- Option(metadata.get(SftNameKey))
      spec <- Option(metadata.get(SftSpecKey))
    } yield {
      val sft = SimpleFeatureTypes.createType(name, spec)
      val geometries = Option(metadata.get(GeometryEncodingKey)).map(GeometryEncoding.apply).getOrElse {
        // noinspection ScalaDeprecation
        if (metadata.get(SchemaVersionKey) == "1") { GeometryEncoding.GeoMesaV1 } else { GeometryEncoding.GeoMesaV0 }
      }
      val encodings = Encodings(geometries)
      val bboxes = fileSchema.getFields.asScala.map(_.getName).flatMap(BoundingBoxField.fromBoundingBox).toSeq
      val visibilities = fileSchema.containsField(VisibilitiesField)
      SimpleFeatureParquetSchema(sft, encodings, visibilities, bboxes, Collections.unmodifiableMap(metadata))
    }
  }

  /**
   * Gets a schema for writing
   *
   * @param sftName sft name config
   * @param sftSpec sft spec config
   * @param geoms geometry encoding config
   * @param bboxOpt bounding box encoding config
   * @param vis visibility config
   * @return
   */
  private def write(sftName: String, sftSpec: String, geoms: String, bboxOpt: String, vis: String): Option[SimpleFeatureParquetSchema] = {
    for {
      name <- Option(sftName)
      spec <- Option(sftSpec)
    } yield {
      val sft = SimpleFeatureTypes.createType(name, spec)
      val geometries = Option(geoms).map(GeometryEncoding.apply).getOrElse(GeometryEncoding.GeoParquetWkb)
      // only include bboxes if they help with push-down filters
      val bboxes = if (bboxOpt != null && !bboxOpt.toBoolean) { Seq.empty } else { BoundingBoxField(sft, geometries) }
      val visibilities = Option(sft.getUserData.get(VisibilityEncodingKey)).orElse(Option(vis)).forall(_.toString.toBoolean)
      val metadata = new java.util.HashMap[String, String]()
      metadata.put(SftNameKey, name)
      metadata.put(SftSpecKey, spec)
      metadata.put(GeometryEncodingKey, geometries.toString)
      if (geometries == GeometryEncoding.GeoMesaV1) {
        // noinspection ScalaDeprecation
        metadata.put(SchemaVersionKey, "1") // include schema version for back-compatibility when possible
      }
      val encodings = Encodings(geometries)
      SimpleFeatureParquetSchema(sft, encodings, visibilities, bboxes, Collections.unmodifiableMap(metadata))
    }
  }

  /**
   * Get the message type for a simple feature type
   *
   * @param sft simple feature type
   * @param encodings field type encoding
   * @param bboxes include bounding boxes for each row
   * @param visibilities include visibilities
   * @return
   */
  private def schema(sft: SimpleFeatureType, encodings: Encodings, bboxes: Seq[String], visibilities: Boolean): MessageType = {
    val isV0 = encodings.geometry == GeometryEncoding.GeoMesaV0
    val id = {
      val primitive = PrimitiveTypeName.BINARY
      val builder = if (isV0) { Types.primitive(primitive, Repetition.REPEATED) } else { Types.required(primitive) }
      builder.as(LogicalTypeAnnotation.stringType()).named(FeatureIdField)
    }
    val vis = if (!visibilities) { None } else {
      Some(Types.optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named(VisibilitiesField))
    }
    val boundingBoxes = bboxes.map(BoundingBoxField.schema)
    // note: id field goes at the end of the record, then vis, then any bounding boxes
    val fields = (sft.getAttributeDescriptors.asScala.map(schema(_, encodings)) :+ id) ++ vis ++ boundingBoxes
    val name = if (isV0) { sft.getTypeName } else { alphaNumericSafeString(sft.getTypeName) }
    new MessageType(name, fields.asJava)
  }

  /**
   * Create a parquet field type from an attribute descriptor
   *
   * @param descriptor descriptor
   * @param encodings field type encoding
   * @return
   */
  private def schema(descriptor: AttributeDescriptor, encodings: Encodings): Type = {
    val isV0 = encodings.geometry == GeometryEncoding.GeoMesaV0
    val name = if (isV0) { descriptor.getLocalName } else { alphaNumericSafeString(descriptor.getLocalName) }
    buildType(name, ObjectType.selectType(descriptor), encodings)
  }

  /**
   * Builds the schema type for an attribute
   *
   * @param name field name
   * @param bindings object type
   * @param encodings schema encodings
   * @param repetition repetition
   * @return
   */
  private def buildType(
      name: String,
      bindings: Seq[ObjectType],
      encodings: Encodings,
      repetition: Repetition = Repetition.OPTIONAL): Type = {
    val builder = bindings.head match {
      case ObjectType.STRING   => Types.primitive(PrimitiveTypeName.BINARY, repetition).as(LogicalTypeAnnotation.stringType())
      case ObjectType.INT      => Types.primitive(PrimitiveTypeName.INT32, repetition)
      case ObjectType.DOUBLE   => Types.primitive(PrimitiveTypeName.DOUBLE, repetition)
      case ObjectType.LONG     => Types.primitive(PrimitiveTypeName.INT64, repetition)
      case ObjectType.FLOAT    => Types.primitive(PrimitiveTypeName.FLOAT, repetition)
      case ObjectType.BOOLEAN  => Types.primitive(PrimitiveTypeName.BOOLEAN, repetition)
      case ObjectType.BYTES    => Types.primitive(PrimitiveTypeName.BINARY, repetition)

      case ObjectType.DATE =>
        val unit = encodings.date match {
          case DateEncoding.Micros => TimeUnit.MICROS
          case DateEncoding.Millis => TimeUnit.MILLIS
          case _ => throw new UnsupportedOperationException(encodings.date.toString)
        }
        Types.primitive(PrimitiveTypeName.INT64, repetition).as(LogicalTypeAnnotation.timestampType(true, unit))

      case ObjectType.UUID =>
        encodings.uuid match {
          case UuidEncoding.FixedLength =>
            Types.primitive(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, repetition).length(16).as(LogicalTypeAnnotation.uuidType())
          case UuidEncoding.Bytes => Types.primitive(PrimitiveTypeName.BINARY, repetition)
          case _ => throw new UnsupportedOperationException(encodings.uuid.toString)
        }

      case ObjectType.LIST =>
        encodings.list match {
          case ListEncoding.ThreeLevel =>
            Types.optionalList().requiredGroupElement().addField(buildType("element", bindings.drop(1), encodings, Repetition.REQUIRED))
          case ListEncoding.TwoLevel => Types.optionalList().element(buildType("element", bindings.drop(1), encodings))
          case _ => throw new UnsupportedOperationException(encodings.list.toString)
        }

      case ObjectType.MAP =>
        Types.optionalMap()
          .key(buildType("key", bindings.slice(1, 2), encodings, Repetition.REQUIRED))
          .value(buildType("value", bindings.slice(2, 3), encodings))

      case ObjectType.GEOMETRY =>
        GeometrySchema(bindings(1), encodings.geometry)

      case binding =>
        throw new UnsupportedOperationException(s"No mapping defined for type $binding")
    }
    builder.named(name)
  }
}
