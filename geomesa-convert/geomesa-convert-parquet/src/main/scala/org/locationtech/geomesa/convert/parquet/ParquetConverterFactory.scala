/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.parquet

import com.typesafe.config.Config
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.{MessageType, OriginalType, Type}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert2.AbstractConverter.{BasicConfig, BasicField, BasicOptions}
import org.locationtech.geomesa.convert2.AbstractConverterFactory.{BasicConfigConvert, BasicFieldConvert, BasicOptionsConvert}
import org.locationtech.geomesa.convert2.TypeInference.{FunctionTransform, InferredType, Namer}
import org.locationtech.geomesa.convert2.transforms.Expression
import org.locationtech.geomesa.convert2.{AbstractConverterFactory, TypeInference}
import org.locationtech.geomesa.fs.storage.parquet.io.SimpleFeatureParquetSchema
import org.locationtech.geomesa.utils.geotools.ObjectType
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
        val (schema, fields, id) = SimpleFeatureParquetSchema.read(footer.getFileMetaData) match {
          case Some(parquet) =>
            // this is a geomesa encoded parquet file
            val fields = parquet.sft.getAttributeDescriptors.asScala.map { descriptor =>
              val name = parquet.field(parquet.sft.indexOf(descriptor.getLocalName))
              // note: parquet converter stores the generic record under index 0
              val path = s"avroPath($$0, '/$name')"
              // some types need a function applied to the underlying avro value
              val expression = ObjectType.selectType(descriptor) match {
                case Seq(ObjectType.GEOMETRY, ObjectType.POINT)           => s"parquetPoint($$0, '/$name')"
                case Seq(ObjectType.GEOMETRY, ObjectType.MULTIPOINT)      => s"parquetMultiPoint($$0, '/$name')"
                case Seq(ObjectType.GEOMETRY, ObjectType.LINESTRING)      => s"parquetLineString($$0, '/$name')"
                case Seq(ObjectType.GEOMETRY, ObjectType.MULTILINESTRING) => s"parquetMultiLineString($$0, '/$name')"
                case Seq(ObjectType.GEOMETRY, ObjectType.POLYGON)         => s"parquetPolygon($$0, '/$name')"
                case Seq(ObjectType.GEOMETRY, ObjectType.MULTIPOLYGON)    => s"parquetMultiPolygon($$0, '/$name')"
                case Seq(ObjectType.UUID)                                 => s"avroBinaryUuid($path)"
                case _                                                    => path
              }
              BasicField(descriptor.getLocalName, Some(Expression(expression)))
            }
            val id = Expression(s"avroPath($$0, '/${SimpleFeatureParquetSchema.FeatureIdField}')")

            // validate the existing schema, if any
            if (sft.exists(_.getAttributeDescriptors.asScala != parquet.sft.getAttributeDescriptors.asScala)) {
              throw new IllegalArgumentException("Inferred schema does not match existing schema")
            }
            (parquet.sft, fields, Some(id))

          case _ =>
            // this is an arbitrary parquet file, create fields based on the schema
            val types = ParquetConverterFactory.schemaTypes(footer.getFileMetaData.getSchema)
            val dataSft = TypeInference.schema("inferred-parquet", types)
            // note: parquet converter stores the generic record under index 0
            val fields = types.map(t => BasicField(t.name, Some(Expression(t.transform.apply(0)))))

            // validate the existing schema, if any
            sft.foreach(AbstractConverterFactory.validateInferredType(_, types.map(_.typed)))

            (dataSft, fields, None)
        }

        val converterConfig = BasicConfig(typeToProcess, id, Map.empty, Map.empty)

        val config = configConvert.to(converterConfig)
            .withFallback(fieldConvert.to(fields.toSeq))
            .withFallback(optsConvert.to(BasicOptions.default))
            .toConfig

        (schema, config)
      }
    }
  }
}

object ParquetConverterFactory {

  import scala.collection.JavaConverters._

  val TypeToProcess = "parquet"

  /**
    * Take an avro schema and return the simple feature type bindings for it
    *
    * @param schema avro schema
    * @return
    */
  def schemaTypes(schema: MessageType): Seq[InferredType] = {
    val namer = new Namer()
    val types = scala.collection.mutable.ArrayBuffer.empty[InferredType]

    def mapField(field: Type, path: String = ""): Unit = {
      // get a valid attribute name
      val name = namer(field.getName)
      val transform = FunctionTransform("avroPath(", s",'$path/${field.getName}')")

      val original = field.getOriginalType
      if (field.isPrimitive) {
        // note: date field transforms are handled by AvroReadSupport
        lazy val int32: InferredType = original match {
          case OriginalType.DATE => InferredType(name, ObjectType.DATE, transform)
          case _ => InferredType(name, ObjectType.INT, transform)
        }
        lazy val int64: InferredType = original match {
          case OriginalType.TIMESTAMP_MILLIS => InferredType(name, ObjectType.DATE, transform)
          case OriginalType.TIMESTAMP_MICROS => InferredType(name, ObjectType.DATE, transform)
          case _ => InferredType(name, ObjectType.LONG, transform)
        }
        lazy val binary: InferredType = original match {
          case OriginalType.UTF8 => InferredType(name, ObjectType.STRING, transform)
          case _ => InferredType(name, ObjectType.BYTES, transform)
        }

        field.asPrimitiveType().getPrimitiveTypeName match {
          case PrimitiveTypeName.BINARY               => types += binary
          case PrimitiveTypeName.INT32                => types += int32
          case PrimitiveTypeName.INT64                => types += int64
          case PrimitiveTypeName.FLOAT                => types += InferredType(name, ObjectType.FLOAT, transform)
          case PrimitiveTypeName.DOUBLE               => types += InferredType(name, ObjectType.DOUBLE, transform)
          case PrimitiveTypeName.BOOLEAN              => types += InferredType(name, ObjectType.BOOLEAN, transform)
          case PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY => types += InferredType(name, ObjectType.BYTES, transform)
          case _ => // no-op
        }
      } else {
        val group = field.asGroupType()
        original match {
          case OriginalType.LIST =>
            if (group.getFieldCount == 1 && !group.getType(0).isPrimitive) {
              val list = group.getType(0).asGroupType()
              if (list.getFieldCount == 1 && list.isRepetition(Repetition.REPEATED) && list.getType(0).isPrimitive) {
                types += InferredType(name, ObjectType.LIST, transform)
              }
            }

          case OriginalType.MAP =>
            if (group.getFieldCount == 1 && !group.getType(0).isPrimitive) {
              val map = group.getType(0).asGroupType()
              if (map.getFieldCount == 2 && map.isRepetition(Repetition.REPEATED) &&
                  map.getFields.asScala.forall(_.isPrimitive)) {
                types += InferredType(name, ObjectType.MAP, transform)
              }
            }

          case _ =>
            if (group.getFieldCount == 1 && group.getType(0).isPrimitive &&
                group.getType(0).isRepetition(Repetition.REPEATED)) {
              types += InferredType(name, ObjectType.LIST, transform)
            } else {
              group.getFields.asScala.foreach(mapField(_, s"$path/${field.getName}"))
            }
        }
      }
    }

    schema.getFields.asScala.foreach(mapField(_))

    // check if we can derive a geometry field
    TypeInference.deriveGeometry(types.toSeq, namer).foreach(g => types += g)

    types.toSeq
  }
}
