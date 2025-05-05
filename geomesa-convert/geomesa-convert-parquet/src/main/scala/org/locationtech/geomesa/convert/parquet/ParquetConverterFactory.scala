/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.parquet

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.FileMetaData
import org.apache.parquet.schema.LogicalTypeAnnotation._
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.{GroupType, LogicalTypeAnnotation, Type}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert2.AbstractConverter.{BasicConfig, BasicField, BasicOptions}
import org.locationtech.geomesa.convert2.AbstractConverterFactory.{BasicConfigConvert, BasicFieldConvert, BasicOptionsConvert}
import org.locationtech.geomesa.convert2.TypeInference.{FunctionTransform, InferredType, Namer}
import org.locationtech.geomesa.convert2.transforms.Expression
import org.locationtech.geomesa.convert2.{AbstractConverterFactory, TypeInference}
import org.locationtech.geomesa.fs.storage.parquet.io.GeoParquetMetadata.{ColumnMetadata, GeoParquetColumnEncoding, GeoParquetColumnType}
import org.locationtech.geomesa.fs.storage.parquet.io.GeometrySchema.GeometryEncoding.GeoParquetNative
import org.locationtech.geomesa.fs.storage.parquet.io.{GeoParquetMetadata, GeometrySchema, SimpleFeatureParquetSchema}
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.geotools.ObjectType
import org.locationtech.geomesa.utils.geotools.ObjectType.ObjectType
import org.locationtech.geomesa.utils.io.PathUtils

import java.io.InputStream
import scala.util.{Failure, Success, Try}

class ParquetConverterFactory
    extends AbstractConverterFactory[ParquetConverter, BasicConfig, BasicField, BasicOptions](
      ParquetConverterFactory.TypeToProcess, BasicConfigConvert, BasicFieldConvert, BasicOptionsConvert) {

  import scala.collection.JavaConverters._

  /**
    * Handles parquet files (including those produced by the FSDS and CLI export)
    *
    * @param is input
    * @param sft simple feature type, if known ahead of time
    * @param path file path, if there is a file available
    * @return
    */
  override def infer(
      is: InputStream,
      sft: Option[SimpleFeatureType],
      path: Option[String]): Option[(SimpleFeatureType, Config)] =
    infer(is, sft, path.fold(Map.empty[String, AnyRef])(EvaluationContext.inputFileParam)).toOption

  override def infer(
      is: InputStream,
      sft: Option[SimpleFeatureType],
      hints: Map[String, AnyRef]): Try[(SimpleFeatureType, Config)] = {
    val path = hints.get(EvaluationContext.InputFilePathKey) match {
      case None => Failure(new RuntimeException("No file path specified to the input data"))
      case Some(p) => Success(p.toString)
    }
    path.flatMap { p =>
      Try {
        // note: get the path as a URI so that we handle local files appropriately
        val filePath = new Path(PathUtils.getUrl(p).toURI)
        val footer = ParquetFileReader.readFooter(new Configuration(), filePath, ParquetMetadataConverter.NO_FILTER)
        val (schema, fields, id, userData) = SimpleFeatureParquetSchema.read(footer.getFileMetaData) match {
          case Some(parquet) =>
            // this is a geomesa encoded parquet file
            val fields = parquet.sft.getAttributeDescriptors.asScala.map { descriptor =>
              val name = parquet.field(parquet.sft.indexOf(descriptor.getLocalName))
              // note: parquet converter stores the generic record under index 0
              BasicField(descriptor.getLocalName, Some(Expression(s"avroPath($$0, '/$name')")))
            }
            val id = Expression(s"avroPath($$0, '/${SimpleFeatureParquetSchema.FeatureIdField}')")
            val userData =
              if (parquet.hasVisibilities) {
                Map(SecurityUtils.FEATURE_VISIBILITY -> Expression(s"avroPath($$0, '/${SimpleFeatureParquetSchema.VisibilitiesField}')"))
              } else {
                Map.empty[String, Expression]
              }

            // validate the existing schema, if any
            if (sft.exists(_.getAttributeDescriptors.asScala != parquet.sft.getAttributeDescriptors.asScala)) {
              throw new IllegalArgumentException("Inferred schema does not match existing schema")
            }
            (parquet.sft, fields, Some(id), userData)

          case _ =>
            // this is an arbitrary parquet file, create fields based on the schema
            val types = ParquetConverterFactory.schemaTypes(footer.getFileMetaData)
            val dataSft = TypeInference.schema("inferred-parquet", types)
            // note: parquet converter stores the generic record under index 0
            val fields = types.map(t => BasicField(t.name, Some(Expression(t.transform.apply(0)))))

            // validate the existing schema, if any
            sft.foreach(AbstractConverterFactory.validateInferredType(_, types.map(_.typed)))

            (dataSft, fields, None, Map.empty[String, Expression])
        }

        val converterConfig = BasicConfig(typeToProcess, id, Map.empty, userData)

        val config = configConvert.to(converterConfig)
            .withFallback(fieldConvert.to(fields.toSeq))
            .withFallback(optsConvert.to(BasicOptions.default))
            .toConfig

        (schema, config)
      }
    }
  }
}

object ParquetConverterFactory extends LazyLogging {

  import scala.collection.JavaConverters._

  val TypeToProcess = "parquet"

  /**
   * Gets the type of elements in this list, assuming that this is a standard, 3-level parquet list type
   *
   * @param group list type
   * @return element type, if input is a properly formatted 3-level parquet list
   */
  def getListElementType(group: GroupType): Option[GroupType] = {
    for {
      g <- Some(group) // optional group foo (LIST) {
      if g.getFieldCount == 1 && !g.getType(0).isPrimitive
      list = g.getType(0).asGroupType() // repeated group list {
      if list.getFieldCount == 1 && list.isRepetition(Repetition.REPEATED) && !list.getType(0).isPrimitive
    } yield {
      list.getType(0).asGroupType() // optional group element {
    }
  }

  /**
   * Gets the type of keys and values in this map, assuming that this is a standard, 3-level parquet map type
   *
   * @param map map type
   * @return (key type, value type), if input is a properly formatted 3-level parquet map
   */
  def getMapKeyValueTypes(map: GroupType): Option[(Type, Type)] = {
    for {
      mapGroup <- Some(map)
      if mapGroup.getFieldCount == 1
      tuples = mapGroup.getType(0)
      if !tuples.isPrimitive
      tuplesGroup = tuples.asGroupType()
      if tuplesGroup.getFieldCount == 2 && tuplesGroup.isRepetition(Repetition.REPEATED)
    } yield {
      (tuplesGroup.getType(0), tuplesGroup.getType(1))
    }
  }

  /**
    * Takes parquet file info returns the simple feature type bindings for it
    *
    * @param fileMetaData parquet file footer
    * @return
    */
  private def schemaTypes(fileMetaData: FileMetaData): Seq[InferredType] = {
    val namer = new Namer()
    val geos: Option[GeoParquetMetadata] =
      Option(fileMetaData.getKeyValueMetaData.get(GeoParquetMetadata.GeoParquetMetadataKey)).map(GeoParquetMetadata.fromJson)

    val types = fileMetaData.getSchema.getFields.asScala.flatMap(mapField(_, namer, geos))
    // check if we can derive a geometry field
    val geom = TypeInference.deriveGeometry(types, namer)
    types ++ geom
  }

  /**
   * Infer a type from a top-level parquet field
   *
   * @param field field
   * @param namer name helper
   * @param geos extracted GeoParquet metadata from the parquet file
   * @return
   */
  private def mapField(field: Type, namer: Namer, geos: Option[GeoParquetMetadata]): Seq[InferredType] = {
    if (geos.exists(_.geometries.exists(_.covering.contains(field.getName)))) {
      return Seq.empty // ignore bbox fields
    }
    // note: geometries *must* be at the root level, so we don't need to track them for nested fields
    geos.flatMap(_.geometries.find(_.name == field.getName)) match {
      case None => mapNormalField(field, namer)
      case Some(geo) => mapGeoField(field, namer, geo)
    }
  }

  /**
   * Maps a non-geometry type field
   *
   * @param field field
   * @param namer name helper
   * @param path avro path to the parent of the field
   * @return
   */
  private def mapNormalField(field: Type, namer: Namer, path: String = ""): Seq[InferredType] = {
    val fieldPath = s"$path/${field.getName}"
    lazy val name = namer(fieldPath)
    val transform = FunctionTransform("avroPath(", s",'$fieldPath')")
    val logical = field.getLogicalTypeAnnotation
    if (field.isPrimitive) {
      // note: date field transforms are handled by AvroReadSupport
      lazy val intOrDate: InferredType = logical match {
        case _: DateLogicalTypeAnnotation => InferredType(name, ObjectType.DATE, transform)
        case _ => InferredType(name, ObjectType.INT, transform)
      }
      lazy val longOrTimestamp: InferredType = logical match {
        case t: TimestampLogicalTypeAnnotation if t.getUnit == LogicalTypeAnnotation.TimeUnit.MILLIS => InferredType(name, ObjectType.DATE, transform)
        case t: TimestampLogicalTypeAnnotation if t.getUnit == LogicalTypeAnnotation.TimeUnit.MICROS => InferredType(name, ObjectType.DATE, transform)
        case _ => InferredType(name, ObjectType.LONG, transform)
      }
      lazy val stringOrBytes: InferredType = logical match {
        case _: StringLogicalTypeAnnotation => InferredType(name, ObjectType.STRING, transform)
        case _ => InferredType(name, ObjectType.BYTES, transform)
      }
      field.asPrimitiveType().getPrimitiveTypeName match {
        case PrimitiveTypeName.BINARY               => Seq(stringOrBytes)
        case PrimitiveTypeName.INT32                => Seq(intOrDate)
        case PrimitiveTypeName.INT64                => Seq(longOrTimestamp)
        case PrimitiveTypeName.FLOAT                => Seq(InferredType(name, ObjectType.FLOAT, transform))
        case PrimitiveTypeName.DOUBLE               => Seq(InferredType(name, ObjectType.DOUBLE, transform))
        case PrimitiveTypeName.BOOLEAN              => Seq(InferredType(name, ObjectType.BOOLEAN, transform))
        case PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY => Seq(InferredType(name, ObjectType.BYTES, transform))
        case _ => Seq.empty
      }
    } else {
      val group = field.asGroupType()
      def jsonTransform(): FunctionTransform = FunctionTransform(s"avroToJson(${transform.prefix}", s"${transform.suffix})")
      logical match {
        case _: ListLogicalTypeAnnotation =>
          if (getListElementType(group).exists(e => e.getFieldCount == 1 && e.getType(0).isPrimitive)) {
            Seq(InferredType(name, ObjectType.LIST, transform))
          } else {
            // for non-standard or complex-element lists, we just convert to a json string
            Seq(InferredType(name, ObjectType.JSON, jsonTransform()))
          }

        case _: MapLogicalTypeAnnotation =>
          if (getMapKeyValueTypes(group).exists { case (k, v) => k.isPrimitive && v.isPrimitive }) {
            Seq(InferredType(name, ObjectType.MAP, transform))
          } else {
            // for non-standard or complex-value maps, we just convert to a json string
            Seq(InferredType(name, ObjectType.JSON, jsonTransform()))
          }

        case _ =>
          group.getFields.asScala.flatMap(mapNormalField(_, namer, fieldPath))
      }
    }
  }

  /**
   * Maps a GeoParquet geometry field
   *
   * @param field field
   * @param namer name helper
   * @param geo GeoParquet metadata
   * @return
   */
  private def mapGeoField(field: Type, namer: Namer, geo: ColumnMetadata): Seq[InferredType] = {
    lazy val name = namer(field.getName)

    def geomTransform(fn: String): FunctionTransform = FunctionTransform(s"$fn(avroPath(", s",'/${field.getName}'))")

    def mismatch(): Seq[InferredType] = {
      logger.warn(s"Found GeoParquet metadata for column '${field.getName}' but the schema does not match the expected type")
      mapNormalField(field, namer)
    }

    if (geo.encoding == GeoParquetColumnEncoding.WKB) {
      if (isWkbType(field)) {
        Seq(InferredType(name, GeoParquetColumnType.toObjectType(geo.types), geomTransform("geometry")))
      } else {
        mismatch()
      }
    } else {
      val objectType = GeoParquetColumnEncoding.toObjectType(geo.encoding)
      if ((geo.types.isEmpty || GeoParquetColumnType.toObjectType(geo.types) == objectType) && isNativeType(field, objectType)) {
        Seq(InferredType(name, objectType, geomTransform(geo.encoding.toString)))
      } else {
        mismatch()
      }
    }
  }

  /**
   * Checks if a field corresponds to the GeoParquet schema for a WKB-encoded geometry
   *
   * @param field field
   * @return
   */
  private def isWkbType(field: Type): Boolean =
    !field.isRepetition(Repetition.REPEATED) && field.isPrimitive &&
      field.asPrimitiveType().getPrimitiveTypeName == PrimitiveTypeName.BINARY

  /**
   * Checks if the field corresponds to the GeoParquet schema for a native-encoded geometry
   *
   * @param field field
   * @param objectType native type
   * @return
   */
  private def isNativeType(field: Type, objectType: ObjectType): Boolean = {
    // compare child fields, so we don't consider optional vs required, id, name, logicalTypes, etc
    !field.isRepetition(Repetition.REPEATED) &&
      field.asGroupType().getFields == GeometrySchema(objectType, GeoParquetNative).named(field.getName).asGroupType().getFields
  }
}
