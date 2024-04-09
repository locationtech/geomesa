/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.avro

import com.typesafe.config.Config
import org.apache.avro.Schema
import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert.avro.AvroConverter._
import org.locationtech.geomesa.convert.avro.AvroConverterFactory.AvroConfigConvert
import org.locationtech.geomesa.convert2.AbstractConverter.{BasicField, BasicOptions}
import org.locationtech.geomesa.convert2.AbstractConverterFactory.{BasicFieldConvert, BasicOptionsConvert, ConverterConfigConvert, OptionConvert}
import org.locationtech.geomesa.convert2.TypeInference.{FunctionTransform, InferredType, Namer}
import org.locationtech.geomesa.convert2.transforms.Expression
import org.locationtech.geomesa.convert2.{AbstractConverterFactory, TypeInference}
import org.locationtech.geomesa.features.avro.io.AvroDataFile
import org.locationtech.geomesa.features.avro.serialization.AvroField.{FidField, UserDataField, VersionField}
import org.locationtech.geomesa.features.avro.serialization.AvroSerialization
import org.locationtech.geomesa.features.avro.{FieldNameEncoder, SerializationVersions}
import org.locationtech.geomesa.utils.geotools.ObjectType
import org.locationtech.geomesa.utils.io.WithClose
import pureconfig.ConfigObjectCursor
import pureconfig.error.{ConfigReaderFailures, FailureReason}

import java.io.InputStream
import scala.util.Try

class AvroConverterFactory extends AbstractConverterFactory[AvroConverter, AvroConfig, BasicField, BasicOptions](
  "avro", AvroConfigConvert, BasicFieldConvert, BasicOptionsConvert) {

  import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichIterator

  import scala.collection.JavaConverters._

  /**
    * Note: only works on Avro files with embedded schemas
    *
    * @param is input
    * @param sft simple feature type, if known ahead of time
    * @param path file path, if known
    * @return
    */
  override def infer(
      is: InputStream,
      sft: Option[SimpleFeatureType],
      path: Option[String]): Option[(SimpleFeatureType, Config)] =
    infer(is, sft, path.map(EvaluationContext.inputFileParam).getOrElse(Map.empty)).toOption

  override def infer(
      is: InputStream,
      sft: Option[SimpleFeatureType],
      hints: Map[String, AnyRef]): Try[(SimpleFeatureType, Config)] = {
    Try {
      WithClose(new DataFileStream[GenericRecord](is, new GenericDatumReader[GenericRecord]())) { dfs =>
        val (schema, id, fields, userData) = if (AvroDataFile.canParse(dfs)) {
          // this is a file written in the geomesa avro format
          val records = dfs.iterator.asScala.take(AbstractConverterFactory.inferSampleSize)
          // get the version from the first record
          val version =
            records.headOption
                .flatMap(r => Option(r.get(VersionField.name)).map(_.asInstanceOf[Int]))
                .getOrElse(SerializationVersions.DefaultVersion)
          val nameEncoder = new FieldNameEncoder(version)
          val dataSft = AvroDataFile.getSft(dfs)
          val native = AvroSerialization.usesNativeCollections(dfs.getSchema)

          val fields = dataSft.getAttributeDescriptors.asScala.map { descriptor =>
            // some types need a function applied to the underlying avro value
            val fn = ObjectType.selectType(descriptor).head match {
              case ObjectType.DATE            => Some("millisToDate")
              case ObjectType.UUID            => Some("avroBinaryUuid")
              case ObjectType.GEOMETRY        => Some("geometry") // note: handles both wkt (v1) and wkb (v2)
              case ObjectType.LIST if !native => Some("avroBinaryList")
              case ObjectType.MAP if !native  => Some("avroBinaryMap")
              case _ => None
            }

            val path = s"avroPath($$1, '/${nameEncoder.encode(descriptor.getLocalName)}')"
            val expression = fn.map(f => s"$f($path)").getOrElse(path)
            BasicField(descriptor.getLocalName, Some(Expression(expression)))
          }

          val id = Expression(s"avroPath($$1, '/${FidField.name}')")
          val userData: Map[String, Expression] =
            if (dfs.getSchema.getField(UserDataField.name) == null) { Map.empty } else {
              // avro user data is stored as an array of 'key', 'keyClass', 'value', and 'valueClass'
              // our converters require global key->expression, so pull out the unique keys
              val kvs = scala.collection.mutable.Map.empty[String, Expression]
              records.foreach { record =>
                val ud = record.get(UserDataField.name).asInstanceOf[java.util.Collection[GenericRecord]]
                ud.asScala.foreach { rec =>
                  if (rec.getSchema.getField("key") != null) {
                    Option(rec.get("key")).map(_.toString).foreach { key =>
                      kvs.getOrElseUpdate(key, {
                        var expression = s"avroPath($$1, '/${UserDataField.name}[$$key=$key]/value')"
                        if (rec.getSchema.getField("valueClass") != null &&
                            Option(rec.get("valueClass")).map(_.toString).contains("java.util.Date")) {
                          // dates have to be converted from millis
                          expression = s"millisToDate($expression)"
                        }
                        Expression(expression)
                      })
                    }
                  }
                }
              }
              kvs.toMap
            }

          (dataSft, id, fields, userData)
        } else {
          // this is an arbitrary avro file, create fields based on the schema
          val types = AvroConverterFactory.schemaTypes(dfs.getSchema)
          val dataSft = TypeInference.schema("inferred-avro", types)
          // note: avro values are always stored at index 1
          val id = Expression("md5(string2bytes($1::string))")
          val fields = types.map(t => BasicField(t.name, Some(Expression(t.transform.apply(1)))))

          (dataSft, id, fields, Map.empty[String, Expression])
        }

        // validate the existing schema, if any
        sft.foreach { existing =>
          val inferred = schema.getAttributeDescriptors.asScala
          val matched = existing.getAttributeDescriptors.asScala.count { d =>
            inferred.exists { i =>
              i.getLocalName == d.getLocalName && d.getType.getBinding.isAssignableFrom(i.getType.getBinding)
            }
          }
          if (matched == 0) {
            throw new IllegalArgumentException("Inferred schema does not match existing schema")
          } else if (matched < existing.getAttributeCount) {
            logger.warn(s"Inferred schema only matched $matched out of ${existing.getAttributeCount} attributes")
          }
        }

        val converterConfig = AvroConfig(typeToProcess, SchemaEmbedded, Some(id), Map.empty, userData)

        val config = configConvert.to(converterConfig)
            .withFallback(fieldConvert.to(fields.toSeq))
            .withFallback(optsConvert.to(BasicOptions.default))
            .toConfig

        (schema, config)
      }
    }
  }
}

object AvroConverterFactory {

  import scala.collection.JavaConverters._

  /**
    * Take an avro schema and return the simple feature type bindings for it
    *
    * @param schema avro schema
    * @return
    */
  def schemaTypes(schema: Schema): Seq[InferredType] = {
    val namer = new Namer()
    val types = scala.collection.mutable.ArrayBuffer.empty[InferredType]

    def mapField(field: Schema.Field, path: String = ""): Unit = {
      // get a valid attribute name
      val name = namer(field.name())

      // checks for nested array/map types we can handle
      def isSimpleArray: Boolean = isSimple(field.schema().getElementType)
      def isSimpleMap: Boolean = isSimple(field.schema().getValueType)

      val transform = FunctionTransform("avroPath(", s",'$path/${field.name}')")
      field.schema().getType match {
        case Schema.Type.STRING  => types += InferredType(name, ObjectType.STRING, transform)
        case Schema.Type.BYTES   => types += InferredType(name, ObjectType.BYTES, transform)
        case Schema.Type.INT     => types += InferredType(name, ObjectType.INT, transform)
        case Schema.Type.LONG    => types += InferredType(name, ObjectType.LONG, transform)
        case Schema.Type.FLOAT   => types += InferredType(name, ObjectType.FLOAT, transform)
        case Schema.Type.DOUBLE  => types += InferredType(name, ObjectType.DOUBLE, transform)
        case Schema.Type.BOOLEAN => types += InferredType(name, ObjectType.BOOLEAN, transform)
        case Schema.Type.ARRAY   => if (isSimpleArray) { types += InferredType(name, ObjectType.LIST, transform) }
        case Schema.Type.MAP     => if (isSimpleMap) { types += InferredType(name, ObjectType.MAP, transform) }
        case Schema.Type.FIXED   => types += InferredType(name, ObjectType.BYTES, transform)
        case Schema.Type.ENUM    => types += InferredType(name, ObjectType.STRING, transform.copy(suffix = transform.suffix + "::string"))
        case Schema.Type.UNION   => types += InferredType(name, ObjectType.STRING, transform.copy(suffix = transform.suffix + "::string"))
        case Schema.Type.RECORD  => field.schema().getFields.asScala.foreach(mapField(_, s"$path/${field.name}"))
        case _ => // no-op
      }
    }

    schema.getFields.asScala.foreach(mapField(_))

    // check if we can derive a geometry field
    TypeInference.deriveGeometry(types.toSeq, namer).foreach(g => types += g)

    types.toSeq
  }

  private def isSimple(s: Schema): Boolean = s.getType match {
    case Schema.Type.STRING  => true
    case Schema.Type.INT     => true
    case Schema.Type.LONG    => true
    case Schema.Type.FLOAT   => true
    case Schema.Type.DOUBLE  => true
    case Schema.Type.BOOLEAN => true
    case _ => false
  }

  object AvroConfigConvert extends ConverterConfigConvert[AvroConfig] with OptionConvert {

    override protected def decodeConfig(
        cur: ConfigObjectCursor,
        `type`: String,
        idField: Option[Expression],
        caches: Map[String, Config],
        userData: Map[String, Expression]): Either[ConfigReaderFailures, AvroConfig] = {

      def schemaOrFile(
          schema: Option[String],
          schemaFile: Option[String]): Either[ConfigReaderFailures, SchemaConfig] = {
        (schema, schemaFile) match {
          case (Some(s), None) if s.equalsIgnoreCase(SchemaEmbedded.name) => Right(SchemaEmbedded)
          case (Some(s), None) => Right(SchemaString(s))
          case (None, Some(s)) => Right(SchemaFile(s))
          case _ =>
            val reason: FailureReason = new FailureReason {
              override val description: String = "Exactly one of 'schema' or 'schema-file' must be defined"
            }
            cur.failed(reason)
        }
      }

      for {
        schema     <- optional(cur, "schema").right
        schemaFile <- optional(cur, "schema-file").right
        either     <- schemaOrFile(schema, schemaFile).right
      } yield {
        AvroConfig(`type`, either, idField, caches, userData)
      }
    }

    override protected def encodeConfig(config: AvroConfig, base: java.util.Map[String, AnyRef]): Unit = {
      config.schema match {
        case SchemaEmbedded  => base.put("schema", SchemaEmbedded.name)
        case SchemaString(s) => base.put("schema", s)
        case SchemaFile(s)   => base.put("schema-file", s)
      }
    }
  }
}
