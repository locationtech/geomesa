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
import org.apache.iceberg.types.Types._
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

  /**
    * Gets the name of the parquet field for the given simple feature type attribute
    *
    * @param i index of the sft attribute
    * @return
    */
  def field(i: Int): String = schema.getFields.get(i).getName

  def iceberg: Schema = SimpleFeatureParquetSchema.icebergSchema(this)
}

object SimpleFeatureParquetSchema extends LazyLogging {

  import StringSerialization.alphaNumericSafeString

  import scala.collection.JavaConverters._

  val FeatureIdField    = "__fid__"
  val VisibilitiesField = "__vis__"

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
    val gmMeta = Seq(SftNameKey, SftSpecKey, SftReadSpecKey, GeometryEncodingKey).flatMap { key =>
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
      val metadata = Map(
        SftNameKey -> name,
        SftSpecKey -> spec,
        GeometryEncodingKey -> geometries.toString,
      ) ++ Option(conf.get(PartitionKey)).map(PartitionKey -> _)
      val encodings = Encodings(geometries)
      val messageType = schema(sft, None, encodings, bboxes, visibilities)
      SimpleFeatureParquetSchema(sft, encodings, visibilities, bboxes, metadata.asJava, messageType)
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
        throw new UnsupportedOperationException("GeoMesaV0/GeoMesaV1 encoding is no longer supported")
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
   * @param readSft optional feature type used to filter the fields in the schema
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
    // note: for iceberg compatibility, field ids need to start at one and increment (without gaps) across all top-level fields.
    // All nested fields (structs, lists) get ids *after* all the top-level fields, once again incrementing without gaps
    val fieldIds = new AtomicInteger(1)
    val id =
      Types.required(PrimitiveTypeName.BINARY)
        .id(fieldIds.getAndIncrement())
        .as(LogicalTypeAnnotation.stringType())
        .named(FeatureIdField)
    val vis = if (!visibilities) { None } else {
      val f =
        Types.optional(PrimitiveTypeName.BINARY)
          .id(fieldIds.getAndIncrement())
          .as(LogicalTypeAnnotation.stringType())
          .named(VisibilitiesField)
      Some(f)
    }
    val attributes = {
      val builder = Map.newBuilder[String, Seq[Type]]
      val nestedFieldIds = new AtomicInteger(fieldIds.get() + sft.getAttributeCount + bboxes.fields.size)
      sft.getAttributeDescriptors.asScala.foreach { d =>
        val name = alphaNumericSafeString(d.getLocalName)
        val types =
          Seq(buildType(name, ObjectType.selectType(d), encodings, fieldIds, nestedFieldIds)) ++
            bboxes.get(d.getLocalName).map(bbox => BoundingBoxField.schema(bbox, fieldIds, nestedFieldIds))
         builder += name -> types
      }
      builder.result()
    }
    val attributeFields = readSft.getOrElse(sft).getAttributeDescriptors.asScala.flatMap { d =>
      attributes(alphaNumericSafeString(d.getLocalName))
    }
    // note: id field goes at the front of the record, then vis, then attributes and bounding boxes
    val fields = Seq(id) ++ vis ++ attributeFields
    val name = alphaNumericSafeString(sft.getTypeName)
    new MessageType(name, fields.asJava)
  }

  private def icebergSchema(schema: SimpleFeatureParquetSchema): Schema = {
    val fields = new java.util.ArrayList[NestedField](schema.sft.getAttributeCount + 3)
    val aliases = new java.util.HashMap[String, Integer](schema.sft.getAttributeCount + 3)

    // see note in `schema` (above) about field ids
    val fieldIds = new AtomicInteger(1)

    aliases.put(FeatureIdField, fieldIds.get())
    fields.add(NestedField.required(fieldIds.getAndIncrement(), FeatureIdField, StringType.get()))

    if (schema.hasVisibilities) {
      aliases.put(VisibilitiesField, fieldIds.get())
      fields.add(NestedField.optional(fieldIds.getAndIncrement(), VisibilitiesField, StringType.get()))
    }

    val nestedFieldIds = new AtomicInteger(fieldIds.get() + schema.sft.getAttributeCount + schema.bboxes.fields.size)
    schema.sft.getAttributeDescriptors.asScala.foreach { d =>
      val name = alphaNumericSafeString(d.getLocalName)
      aliases.put(name, fieldIds.get())
      fields.add(buildIcebergType(name, ObjectType.selectType(d), schema.encodings, fieldIds, nestedFieldIds))
      schema.bboxes.get(d.getLocalName).foreach { bbox =>
        aliases.put(bbox, fieldIds.get())
        fields.add(BoundingBoxField.icebergSchema(bbox, fieldIds, nestedFieldIds))
      }
    }
    new Schema(fields, aliases, java.util.Set.of[Integer](1))
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
          .element(buildType("element", bindings.drop(1), encodings, nestedFieldIds, null /* should not be used*/, Repetition.REQUIRED))

      case ObjectType.MAP =>
        Types.optionalMap()
          .id(fieldIds.getAndIncrement())
          .key(buildType("key", bindings.slice(1, 2), encodings, nestedFieldIds, null /* should not be used*/, Repetition.REQUIRED))
          .value(buildType("value", bindings.slice(2, 3), encodings, nestedFieldIds, null /* should not be used*/))

      case ObjectType.GEOMETRY =>
        encodings.geometry.schema(bindings(1), fieldIds, nestedFieldIds)

      case binding =>
        throw new UnsupportedOperationException(s"No mapping defined for type $binding with encoding $encodings")
    }
    builder.named(name)
  }

  /**
   * Builds the schema type for an attribute
   *
   * @param name field name
   * @param bindings object type
   * @param encodings schema encodings
   * @return
   */
  private def buildIcebergType(
      name: String,
      bindings: Seq[ObjectType],
      encodings: Encodings,
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
      case ObjectType.DATE    => builder.ofType(TimestampType.withoutZone())
      case ObjectType.UUID    => builder.ofType(UUIDType.get())

      case ObjectType.LIST =>
        val subType = buildIcebergType("", bindings.drop(1), encodings, nestedFieldIds, null /* should not be used*/)
        builder.ofType(ListType.ofRequired(subType.fieldId(), subType.`type`()))

      case ObjectType.MAP =>
        val keyType = buildIcebergType("", bindings.slice(1, 2), encodings, nestedFieldIds, null /* should not be used*/)
        val valueType = buildIcebergType("", bindings.slice(2, 3), encodings, nestedFieldIds, null /* should not be used*/)
        builder.ofType(MapType.ofRequired(keyType.fieldId(), valueType.fieldId(), keyType.`type`(), valueType.`type`()))

      case ObjectType.GEOMETRY =>
        // not yet supported in spark or trino
        // builder.ofType(GeometryType.crs84())
        builder.ofType(BinaryType.get())

      case binding =>
        throw new UnsupportedOperationException(s"No mapping defined for type $binding with encoding $encodings")
    }
    typed.build()
  }
}
