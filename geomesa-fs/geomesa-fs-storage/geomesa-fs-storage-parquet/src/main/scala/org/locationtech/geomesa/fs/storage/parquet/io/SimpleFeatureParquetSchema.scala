/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
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
import org.apache.parquet.schema.Types.BasePrimitiveBuilder
import org.apache.parquet.schema._
import org.geotools.api.feature.`type`.AttributeDescriptor
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.fs.storage.common.jobs.StorageConfiguration
import org.locationtech.geomesa.fs.storage.parquet.io.GeometrySchema.GeometryEncoding
import org.locationtech.geomesa.utils.geotools.ObjectType.ObjectType
import org.locationtech.geomesa.utils.geotools.{ObjectType, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.text.StringSerialization

import java.util.Collections

/**
 * A paired simple feature type and parquet schema
 *
 * @param sft simple feature type
 * @param schema parquet message schema
 * @param metadata file metadata
 */
case class SimpleFeatureParquetSchema(sft: SimpleFeatureType, schema: MessageType, metadata: java.util.Map[String, String]) {

  /**
    * Gets the name of the parquet field for the given simple feature type attribute
    *
    * @param i index of the sft attribute
    * @return
    */
  def field(i: Int): String = schema.getFields.get(i).getName

  /**
   * Whether the schema encodes visibilities, or not
   *
   * @return
   */
  def hasVisibilities: Boolean = schema.containsField(SimpleFeatureParquetSchema.VisibilitiesField)
}

object SimpleFeatureParquetSchema extends LazyLogging {

  import StorageConfiguration.{SftNameKey, SftSpecKey}
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

  val GeometryEncodingKey   = "geomesa.parquet.geometries"
  val VisibilityEncodingKey = "geomesa.fs.visibilities"

  val GeometryColumnX = "x"
  val GeometryColumnY = "y"

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
    val conf = context.getParquetConfiguration
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

  private def read(fileSchema: MessageType, metadata: java.util.Map[String, String]): Option[SimpleFeatureParquetSchema] = {
    for {
      name <- Option(metadata.get(SftNameKey))
      spec <- Option(metadata.get(SftSpecKey))
    } yield {
      val sft = SimpleFeatureTypes.createType(name, spec)
      // noinspection ScalaDeprecation
      lazy val schemaVersion = Option(metadata.get(SchemaVersionKey)).map(_.toInt).getOrElse(0)
      val geometryEncoding = Option(metadata.get(GeometryEncodingKey)).map(GeometryEncoding.apply).getOrElse {
        if (schemaVersion == 1) { GeometryEncoding.GeoMesaV1 } else { GeometryEncoding.GeoMesaV0 }
      }
      val visibilities = fileSchema.containsField(VisibilitiesField)
      SimpleFeatureParquetSchema(sft, schema(sft, geometryEncoding, visibilities), Collections.unmodifiableMap(metadata))
    }
  }

  /**
   * Get a schema for writing. Encoding can be configured through `geomesa.parquet.geometries` and `geomesa.fs.visibilities`
   *
   * @param conf write configuration, including the sft spec
   * @return
   */
  def write(conf: Configuration): Option[SimpleFeatureParquetSchema] =
    write(conf.get(SftNameKey), conf.get(SftSpecKey), conf.get(GeometryEncodingKey), conf.get(VisibilityEncodingKey))

  /**
   * Get a schema for writing. Encoding can be configured through `geomesa.parquet.geometries` and `geomesa.fs.visibilities`
   *
   * @param conf write configuration, including the sft spec
   * @return
   */
  def write(conf: ParquetConfiguration): Option[SimpleFeatureParquetSchema] =
    write(conf.get(SftNameKey), conf.get(SftSpecKey), conf.get(GeometryEncodingKey), conf.get(VisibilityEncodingKey))

  /**
   * Gets a schema for writing
   *
   * @param sftName sft name config
   * @param sftSpec sft spec config
   * @param geoms geometry encoding config
   * @param vis visibility config
   * @return
   */
  private def write(sftName: String, sftSpec: String, geoms: String, vis: String): Option[SimpleFeatureParquetSchema] = {
    for {
      name <- Option(sftName)
      spec <- Option(sftSpec)
    } yield {
      val sft = SimpleFeatureTypes.createType(name, spec)
      val geometryEncoding = Option(geoms).map(GeometryEncoding.apply).getOrElse(GeometryEncoding.GeoMesaV1)
      val visibilities = Option(sft.getUserData.get(VisibilityEncodingKey)).orElse(Option(vis)).forall(_.toString.toBoolean)
      val metadata = new java.util.HashMap[String, String]()
      metadata.put(SftNameKey, name)
      metadata.put(SftSpecKey, spec)
      metadata.put(GeometryEncodingKey, geometryEncoding.toString)
      if (geometryEncoding == GeometryEncoding.GeoMesaV1) {
        // noinspection ScalaDeprecation
        metadata.put(SchemaVersionKey, "1") // include schema version for back-compatibility when possible
      }
      SimpleFeatureParquetSchema(sft, schema(sft, geometryEncoding, visibilities), Collections.unmodifiableMap(metadata))
    }
  }

  /**
   * Get the message type for a simple feature type
   *
   * @param sft simple feature type
   * @param geometries geometry encoding
   * @param visibilities include visibilities
   * @return
   */
  private def schema(sft: SimpleFeatureType, geometries: GeometryEncoding, visibilities: Boolean): MessageType = {
    val isV0 = geometries == GeometryEncoding.GeoMesaV0
    val id = {
      val primitive = PrimitiveTypeName.BINARY
      val builder = if (isV0) { Types.primitive(primitive, Repetition.REPEATED) } else { Types.required(primitive) }
      builder.as(LogicalTypeAnnotation.stringType()).named(FeatureIdField)
    }
    val vis =
      if (visibilities) {
        Some(Types.optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named(VisibilitiesField))
      } else {
        None
      }
    // note: id field goes at the end of the record, then vis
    val fields = (sft.getAttributeDescriptors.asScala.map(schema(_, geometries)) :+ id) ++ vis
    val name = if (isV0) { sft.getTypeName } else { alphaNumericSafeString(sft.getTypeName) }
    new MessageType(name, fields.asJava)
  }

  /**
   * Create a parquet field type from an attribute descriptor
   *
   * @param descriptor descriptor
   * @param geometries geometry encoding
   * @return
   */
  private def schema(descriptor: AttributeDescriptor, geometries: GeometryEncoding): Type = {
    val isV0 = geometries == GeometryEncoding.GeoMesaV0
    val bindings = ObjectType.selectType(descriptor)
    val builder = bindings.head match {
      case ObjectType.GEOMETRY => GeometrySchema(bindings(1), geometries)
      case ObjectType.LIST     => Binding(bindings(1), isV0).list()
      case ObjectType.MAP      => Binding(bindings(1), isV0).map(Binding(bindings(2), isV0))
      case p                   => Binding(p, isV0).primitive()
    }
    builder.named(if (isV0) { descriptor.getLocalName } else { alphaNumericSafeString(descriptor.getLocalName) })
  }

  /**
    * Defined parquet field, applicable as primitives, list elements, or map key/values
    *
    * @param name type
    * @param as original type
    * @param length fixed length
    */
  private class Binding(val name: PrimitiveTypeName, as: Option[LogicalTypeAnnotation] = None, length: Option[Int] = None) {

    def primitive(): Types.Builder[_, _ <: Type] = opts(Types.primitive(name, Repetition.OPTIONAL))

    def list(): Types.Builder[_, _ <: Type] = opts(Types.optionalList().optionalElement(name))

    def map(value: Binding): Types.Builder[_, _ <: Type] = {
      val key = opts(Types.optionalMap().key(name))
      value.opts(key.optionalValue(value.name))
    }

    private def opts[T <: BasePrimitiveBuilder[_ <: Type, _]](b: T): T = {
      length.foreach(b.length)
      as.foreach(b.as)
      b
    }
  }

  private object Binding {

    private val bindings = Map(
      ObjectType.DATE    -> new Binding(PrimitiveTypeName.INT64,  Some(LogicalTypeAnnotation.timestampType(true, TimeUnit.MILLIS))),
      ObjectType.STRING  -> new Binding(PrimitiveTypeName.BINARY, Some(LogicalTypeAnnotation.stringType())),
      ObjectType.INT     -> new Binding(PrimitiveTypeName.INT32),
      ObjectType.DOUBLE  -> new Binding(PrimitiveTypeName.DOUBLE),
      ObjectType.LONG    -> new Binding(PrimitiveTypeName.INT64),
      ObjectType.FLOAT   -> new Binding(PrimitiveTypeName.FLOAT),
      ObjectType.BOOLEAN -> new Binding(PrimitiveTypeName.BOOLEAN),
      ObjectType.BYTES   -> new Binding(PrimitiveTypeName.BINARY),
      ObjectType.UUID    -> new Binding(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, None, Some(16))
    )

    def apply(binding: ObjectType, isV0: Boolean): Binding = {
      if (isV0 && binding == ObjectType.UUID) {
        new Binding(PrimitiveTypeName.BINARY)
      } else {
        bindings.getOrElse(binding, throw new NotImplementedError(s"No mapping defined for type $binding"))
      }
    }
  }
}
