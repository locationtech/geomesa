/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
<<<<<<< HEAD
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 1463162d60 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9f430502b2 (GEOMESA-3254 Add Bloop build support)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.fs.storage.parquet.io

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.InitContext
import org.apache.parquet.hadoop.metadata.FileMetaData
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.Types.BasePrimitiveBuilder
import org.apache.parquet.schema._
import org.geotools.api.feature.`type`.AttributeDescriptor
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.features.serialization.TwkbSerialization.GeometryBytes
import org.locationtech.geomesa.fs.storage.common.jobs.StorageConfiguration
import org.locationtech.geomesa.utils.geotools.ObjectType.ObjectType
import org.locationtech.geomesa.utils.geotools.{ObjectType, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.text.StringSerialization

/**
  * A paired simple feature type and parquet schema
  *
  * @param sft simple feature type
  * @param schema parquet message schema
  */
case class SimpleFeatureParquetSchema(sft: SimpleFeatureType, schema: MessageType) {

  import SimpleFeatureParquetSchema.{CurrentSchemaVersion, SchemaVersionKey}

  import scala.collection.JavaConverters._

  /**
    * Parquet file metadata
    */
  lazy val metadata: java.util.Map[String, String] = Map(
    StorageConfiguration.SftNameKey -> sft.getTypeName,
    StorageConfiguration.SftSpecKey -> SimpleFeatureTypes.encodeType(sft, includeUserData = true),
    SchemaVersionKey                -> CurrentSchemaVersion.toString // note: this may not be entirely accurate, but we don't write older versions
  ).asJava

  /**
    * Gets the name of the parquet field for the given simple feature type attribute
    *
    * @param i index of the sft attribute
    * @return
    */
  def field(i: Int): String = schema.getFields.get(i).getName
}

object SimpleFeatureParquetSchema {

  import scala.collection.JavaConverters._

  val FeatureIdField = "__fid__"

  val SchemaVersionKey = "geomesa.parquet.version"

  val CurrentSchemaVersion = 1

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
    // copy in the file level metadata
    context.getKeyValueMetadata.asScala.foreach { case (k, v) => if (!v.isEmpty) { metadata.put(k, v.iterator.next) }}
    val conf = context.getConfiguration
    // copy in the sft from the conf - overwrite the file level metadata as this has our transform schema
    Seq(StorageConfiguration.SftNameKey, StorageConfiguration.SftSpecKey, SchemaVersionKey).foreach { key =>
      val value = conf.get(key)
      if (value != null) {
        metadata.put(key, value)
      }
    }
    apply(metadata)
  }

  /**
    * Extract the simple feature type from an existing parquet file, without any known read context
    *
    * @param footer parquet file footer
    * @return
    */
  def read(footer: FileMetaData): Option[SimpleFeatureParquetSchema] = apply(footer.getKeyValueMetaData)

  /**
    * Get a schema for writing. This will use the latest schema version
    *
    * @param conf write configuration, including the sft spec
    * @return
    */
  def write(conf: Configuration): Option[SimpleFeatureParquetSchema] = {
    val metadata = new java.util.HashMap[String, String]()
    metadata.put(SchemaVersionKey, CurrentSchemaVersion.toString)
    // copy in the sft from the conf
    Seq(StorageConfiguration.SftNameKey, StorageConfiguration.SftSpecKey).foreach { key =>
      val value = conf.get(key)
      if (value != null) {
        metadata.put(key, value)
      }
    }
    apply(metadata)
  }

  /**
    * Determine the appropriate versioned schema
    *
    * @param metadata read metadata, which should include the projected simple feature type and version info
    * @return
    */
  private def apply(metadata: java.util.Map[String, String]): Option[SimpleFeatureParquetSchema] = {
    for {
      name <- Option(metadata.get(StorageConfiguration.SftNameKey))
      spec <- Option(metadata.get(StorageConfiguration.SftSpecKey))
    } yield {
      val sft = SimpleFeatureTypes.createType(name, spec)
      Option(metadata.get(SchemaVersionKey)).map(_.toInt).getOrElse(0) match {
        case 1 => new SimpleFeatureParquetSchema(sft, schema(sft))
        case 0 => new SimpleFeatureParquetSchema(sft, SimpleFeatureParquetSchemaV0(sft))
        case v => throw new IllegalArgumentException(s"Unknown SimpleFeatureParquetSchema version: $v")
      }
    }
  }

  /**
    * Get the message type for a simple feature type
    *
    * @param sft simple feature type
    * @return
    */
  private def schema(sft: SimpleFeatureType): MessageType = {
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

  /**
    * Defined parquet field, applicable as primitives, list elements, or map key/values
    *
    * @param name type
    * @param as original type
    * @param length fixed length
    */
  class Binding(private val name: PrimitiveTypeName, as: Option[OriginalType] = None, length: Option[Int] = None) {

    def primitive(): Types.Builder[_, _ <: Type] = opts(Types.primitive(name, Repetition.OPTIONAL))

    def list(): Types.Builder[_, _ <: Type] = opts(Types.optionalList().optionalElement(name))

    def key(value: ObjectType): Types.Builder[_, _ <: Type] = {
      val key = opts(Types.optionalMap().key(name))
      val values = Binding(value)
      values.opts(key.optionalValue(values.name))
    }

    private def opts[T <: BasePrimitiveBuilder[_ <: Type, _]](b: T): T = {
      length.foreach(b.length)
      as.foreach(b.as)
      b
    }
  }

  object Binding {

    private val bindings = Map(
      ObjectType.DATE    -> new Binding(PrimitiveTypeName.INT64,  Some(OriginalType.TIMESTAMP_MILLIS)),
      ObjectType.STRING  -> new Binding(PrimitiveTypeName.BINARY, Some(OriginalType.UTF8)),
      ObjectType.INT     -> new Binding(PrimitiveTypeName.INT32),
      ObjectType.DOUBLE  -> new Binding(PrimitiveTypeName.DOUBLE),
      ObjectType.LONG    -> new Binding(PrimitiveTypeName.INT64),
      ObjectType.FLOAT   -> new Binding(PrimitiveTypeName.FLOAT),
      ObjectType.BOOLEAN -> new Binding(PrimitiveTypeName.BOOLEAN),
      ObjectType.BYTES   -> new Binding(PrimitiveTypeName.BINARY),
      ObjectType.UUID    -> new Binding(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, None, Some(16))
    )

    def apply(binding: ObjectType): Binding =
      bindings.getOrElse(binding, throw new NotImplementedError(s"No mapping defined for type $binding"))
  }
}
