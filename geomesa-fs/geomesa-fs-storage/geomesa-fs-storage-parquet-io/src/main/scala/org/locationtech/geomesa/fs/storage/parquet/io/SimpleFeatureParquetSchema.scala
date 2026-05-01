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
import org.apache.iceberg.Schema
import org.apache.iceberg.parquet.ParquetSchemaUtil
import org.apache.iceberg.types.Types.NestedField
import org.apache.parquet.conf.{HadoopParquetConfiguration, ParquetConfiguration, PlainParquetConfiguration}
import org.apache.parquet.hadoop.api.InitContext
import org.apache.parquet.hadoop.metadata.FileMetaData
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema._
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.fs.storage.parquet.io.GeometrySchema.{BoundingBoxField, BoundingBoxes, GeometryEncoding}
import org.locationtech.geomesa.utils.geotools.ObjectType.ObjectType
import org.locationtech.geomesa.utils.geotools.{ObjectType, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.text.StringSerialization

import java.util.concurrent.atomic.AtomicInteger

/**
 * A paired simple feature type and parquet schema
 *
 * @param sft simple feature type
 * @param encodings type encoding
 * @param hasVisibilities whether the schema encodes visibilities, or not
 * @param bboxes fields with bounding boxes
 * @param metadata file metadata
 * @param schema parquet message schema
 */
case class SimpleFeatureParquetSchema(
    sft: SimpleFeatureType,
    encodings: Encodings,
    hasVisibilities: Boolean,
    bboxes: BoundingBoxes,
    metadata: java.util.Map[String, String],
    schema: MessageType) {

  import scala.collection.JavaConverters._

  /**
    * Gets the name of the parquet field for the given simple feature type attribute
    *
    * @param i index of the sft attribute
    * @return
    */
  def field(i: Int): String = schema.getFields.get(i).getName

  def iceberg: Schema = {
    val base = ParquetSchemaUtil.convert(schema)
    val optional = base.columns().asScala.map { field =>
      if (field.name() == SimpleFeatureParquetSchema.FeatureIdField) { field } else {
        NestedField.from(field).asOptional().build()
      }
    }
    new Schema(optional.asJava, base.getAliases)
  }
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
  val SftReadSpecKey        = "geomesa.fs.sft.read.spec"
  val GeometryEncodingKey   = "geomesa.parquet.geometries"
  val BBoxEncodingKey       = "geomesa.parquet.bounding-boxes"
  val VisibilityEncodingKey = "geomesa.fs.visibilities"
  val PartitionKey          = "geomesa.fs.partition"

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
  def setSft(conf: ParquetConfiguration, sft: SimpleFeatureType): Unit = {
    val name = Option(sft.getName.getNamespaceURI).map(ns => s"$ns:${sft.getTypeName}").getOrElse(sft.getTypeName)
    conf.set(SftNameKey, name)
    conf.set(SftSpecKey, SimpleFeatureTypes.encodeType(sft, includeUserData = true))
  }

  /**
   * Encodes the feature type in the conf
   *
   * @param conf conf
   * @param sft feature type
   */
  def setReadSft(conf: ParquetConfiguration, sft: SimpleFeatureType): Unit =
    conf.set(SftReadSpecKey, SimpleFeatureTypes.encodeType(sft, includeUserData = true))

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
    val gmMeta = Seq(SftNameKey, SftSpecKey, SftReadSpecKey, GeometryEncodingKey, SchemaVersionKey).flatMap { key =>
      val value =
        try { context.getParquetConfiguration.get(key) } catch {
          // noinspection ScalaDeprecation - use the older method for spark compatibility
          case _: NoSuchMethodError => context.getConfiguration.get(key)
        }
      Option(value).map(key -> _)
    }
    // note: context meta goes second so that transforms override the file-level keys
    read(context.getFileSchema, kvMeta.toMap ++ gmMeta.toMap)
  }

  /**
    * Extract the simple feature type from an existing parquet file, without any known read context
    *
    * @param footer parquet file footer
    * @return
    */
  def read(footer: FileMetaData): Option[SimpleFeatureParquetSchema] =
    read(footer.getSchema, footer.getKeyValueMetaData.asScala.toMap)

  /**
   * Get a schema for writing. Encoding can be configured through `geomesa.parquet.geometries` and `geomesa.fs.visibilities`
   *
   * @param conf write configuration, including the sft spec
   * @return
   */
  def write(conf: Configuration): Option[SimpleFeatureParquetSchema] = write(new HadoopParquetConfiguration(conf))

  /**
   * Get a schema for writing. Encoding can be configured through `geomesa.parquet.geometries` and `geomesa.fs.visibilities`
   *
   * @param conf write configuration, including the sft spec
   * @return
   */
  def write(conf: ParquetConfiguration): Option[SimpleFeatureParquetSchema] = {
    for {
      name <- Option(conf.get(SftNameKey))
      spec <- Option(conf.get(SftSpecKey))
    } yield {
      val sft = SimpleFeatureTypes.createType(name, spec)
      val geometries = Option(conf.get(GeometryEncodingKey)).map(GeometryEncoding.apply).getOrElse(GeometryEncoding.GeoParquetWkb)
      // only include bboxes if they help with push-down filters
      val bboxes =
        if (Option(conf.get(BBoxEncodingKey)).forall(_.toBoolean)) { BoundingBoxes(sft, geometries) } else { BoundingBoxes(Seq.empty) }
      val visibilities =
        Option(sft.getUserData.get(VisibilityEncodingKey)).orElse(Option(conf.get(VisibilityEncodingKey))).forall(_.toString.toBoolean)
      val geomMeta = if (geometries == GeometryEncoding.GeoMesaV1) {
        // noinspection ScalaDeprecation
        Map(SchemaVersionKey -> "1") // include schema version for back-compatibility when possible
      } else {
        Map.empty[String, String]
      }
      val metadata = geomMeta ++ Map(
        SftNameKey -> name,
        SftSpecKey -> spec,
        PartitionKey -> conf.get(PartitionKey),
        GeometryEncodingKey -> geometries.toString,
      )
      val encodings = Encodings(geometries)
      val messageType = schema(sft, None, encodings, bboxes, visibilities)
      SimpleFeatureParquetSchema(sft, encodings, visibilities, bboxes, metadata.asJava, messageType)
    }
  }

  /**
   * Gets the parquet schema
   *
   * @param sft simple feature type
   * @param conf storage configuration
   * @return
   */
  def apply(sft: SimpleFeatureType, conf: Map[String, String]): SimpleFeatureParquetSchema = {
    val config = new PlainParquetConfiguration(conf.asJava)
    setSft(config, sft)
    write(config).get
  }

  /**
   * Get the schema for a file being read
   *
   * @param fileSchema message type
   * @param metadata file metadata
   * @return
   */
  private def read(fileSchema: MessageType, metadata: Map[String, String]): Option[SimpleFeatureParquetSchema] = {
    for {
      name <- metadata.get(SftNameKey)
      spec <- metadata.get(SftSpecKey)
    } yield {
      val sft = SimpleFeatureTypes.createType(name, spec)
      val readSft = metadata.get(SftReadSpecKey).map(SimpleFeatureTypes.createType(name, _))
      val geometries = metadata.get(GeometryEncodingKey).map(GeometryEncoding.apply).getOrElse {
        // noinspection ScalaDeprecation
        if (metadata.get(SchemaVersionKey).contains("1")) { GeometryEncoding.GeoMesaV1 } else {
          throw new UnsupportedOperationException("GeoMesa v0 encoding is no longer supported")
        }
      }
      val encodings = Encodings(geometries)
      val bboxes = BoundingBoxes(fileSchema.getFields.asScala.map(_.getName).flatMap(BoundingBoxField.fromBoundingBox).toSeq)
      val visibilities = fileSchema.containsField(VisibilitiesField)
      val schema = readSchema(fileSchema, sft, readSft, encodings, bboxes, visibilities)
      SimpleFeatureParquetSchema(readSft.getOrElse(sft), encodings, visibilities, bboxes, metadata.asJava, schema)
    }
  }

  /**
   * Gets the projected schema used to read a (possibly transformed) sft
   *
   * @param fileSchema schema of the file being read
   * @param sft feature type corresponding to the file being read
   * @param readSft transformed feature type being read (may the same as the main sft)
   * @param encodings type encoding
   * @param bboxes fields with bounding boxes
   * @param visibilities whether the schema encodes visibilities, or not
   * @return
   */
  private def readSchema(
      fileSchema: MessageType,
      sft: SimpleFeatureType,
      readSft: Option[SimpleFeatureType],
      encodings: Encodings,
      bboxes: BoundingBoxes,
      visibilities: Boolean): MessageType = {
    val consistentSchema = schema(sft, readSft, encodings, bboxes, visibilities) // current schema
    if (fileSchema.getFieldName(0) == SimpleFeatureParquetSchema.FeatureIdField) {
      consistentSchema
    } else {
      def getMappedField(name: String): Type = consistentSchema.getFields.get(consistentSchema.getFieldIndex(name))
      // old files - attributes are first, then fid, vis, bboxes
      val attributes = readSft.getOrElse(sft).getAttributeDescriptors.asScala.map { d =>
        getMappedField(alphaNumericSafeString(d.getLocalName))
      }
      val id = getMappedField(FeatureIdField)
      val vis = if (!visibilities) { None } else { Some(getMappedField(VisibilitiesField)) }
      val boxes = sft.getAttributeDescriptors.asScala.flatMap { d =>
        bboxes.get(d.getLocalName).map(getMappedField)
      }
      val fields = attributes ++ Seq(id) ++ vis ++ boxes
      new MessageType(alphaNumericSafeString(sft.getTypeName), fields.asJava)
    }
  }

  /**
   * Get the message type for a simple feature type. We need the full sft in order to ensure field ids are
   * consistent, but the schema may be reduced (e.g. on read) based on the filter sft
   *
   * @param sft simple feature type
   * @param filter optional feature type used to filter the fields in the schema
   * @param encodings field type encoding
   * @param bboxes include bounding boxes for each row
   * @param visibilities include visibilities
   * @return
   */
  private def schema(
      sft: SimpleFeatureType,
      readSft: Option[SimpleFeatureType],
      encodings: Encodings,
      bboxes: BoundingBoxes,
      visibilities: Boolean): MessageType = {
    val fieldIds = new AtomicInteger(2)
    val id = Types.required(PrimitiveTypeName.BINARY).id(1).as(LogicalTypeAnnotation.stringType()).named(FeatureIdField)
    val vis = if (!visibilities) { None } else {
      fieldIds.incrementAndGet()
      Some(Types.optional(PrimitiveTypeName.BINARY).id(2).as(LogicalTypeAnnotation.stringType()).named(VisibilitiesField))
    }
    val attributes = {
      val builder = Map.newBuilder[String, Seq[Type]]
      sft.getAttributeDescriptors.asScala.foreach { d =>
        val name = alphaNumericSafeString(d.getLocalName)
        val types =
          Seq(buildType(name, ObjectType.selectType(d), encodings, fieldIds)) ++
            bboxes.get(d.getLocalName).map(bbox => BoundingBoxField.schema(bbox, fieldIds))
         builder += name -> types
      }
      builder.result()
    }
    val attributeFields = readSft.getOrElse(sft).getAttributeDescriptors.asScala.flatMap { d =>
      attributes(alphaNumericSafeString(d.getLocalName))
    }
    // note: id field goes at the front of the record, then vis, then any bounding boxes
    val fields = Seq(id) ++ vis ++ attributeFields
    val name = alphaNumericSafeString(sft.getTypeName)
    new MessageType(name, fields.asJava)
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
      fieldIds: AtomicInteger,
      repetition: Repetition = Repetition.OPTIONAL): Type = {
    val builder = bindings.head match {
      case ObjectType.INT      => Types.primitive(PrimitiveTypeName.INT32, repetition).id(fieldIds.getAndIncrement())
      case ObjectType.DOUBLE   => Types.primitive(PrimitiveTypeName.DOUBLE, repetition).id(fieldIds.getAndIncrement())
      case ObjectType.LONG     => Types.primitive(PrimitiveTypeName.INT64, repetition).id(fieldIds.getAndIncrement())
      case ObjectType.FLOAT    => Types.primitive(PrimitiveTypeName.FLOAT, repetition).id(fieldIds.getAndIncrement())
      case ObjectType.BOOLEAN  => Types.primitive(PrimitiveTypeName.BOOLEAN, repetition).id(fieldIds.getAndIncrement())
      case ObjectType.BYTES    => Types.primitive(PrimitiveTypeName.BINARY, repetition).id(fieldIds.getAndIncrement())

      case ObjectType.STRING =>
        Types.primitive(PrimitiveTypeName.BINARY, repetition)
          .id(fieldIds.getAndIncrement())
          .as(LogicalTypeAnnotation.stringType())

      case ObjectType.DATE if encodings.date == DateEncoding.Micros =>
        Types.primitive(PrimitiveTypeName.INT64, repetition)
          .id(fieldIds.getAndIncrement())
          .as(LogicalTypeAnnotation.timestampType(true, TimeUnit.MICROS))

      case ObjectType.DATE if encodings.date == DateEncoding.Millis =>
        Types.primitive(PrimitiveTypeName.INT64, repetition)
          .id(fieldIds.getAndIncrement())
          .as(LogicalTypeAnnotation.timestampType(true, TimeUnit.MILLIS))

      case ObjectType.UUID if encodings.uuid == UuidEncoding.FixedLength =>
        Types.primitive(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, repetition)
          .id(fieldIds.getAndIncrement())
          .length(16)
          .as(LogicalTypeAnnotation.uuidType())

      case ObjectType.LIST if encodings.list == ListEncoding.ThreeLevel =>
        Types.optionalList()
          .id(fieldIds.getAndIncrement())
          .requiredGroupElement()
          .addField(buildType("element", bindings.drop(1), encodings, fieldIds, Repetition.REQUIRED))

      case ObjectType.LIST if encodings.list == ListEncoding.TwoLevel =>
        Types.optionalList()
          .id(fieldIds.getAndIncrement())
          .element(buildType("element", bindings.drop(1), encodings, fieldIds))

      case ObjectType.MAP =>
        Types.optionalMap()
          .id(fieldIds.getAndIncrement())
          .key(buildType("key", bindings.slice(1, 2), encodings, fieldIds, Repetition.REQUIRED))
          .value(buildType("value", bindings.slice(2, 3), encodings, fieldIds))

      case ObjectType.GEOMETRY =>
        GeometrySchema(bindings(1), encodings.geometry, fieldIds)

      case binding =>
        throw new UnsupportedOperationException(s"No mapping defined for type $binding with encoding $encodings")
    }
    builder.named(name)
  }
}
