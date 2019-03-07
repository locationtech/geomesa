/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.avro

import java.io.InputStream
import java.nio.charset.StandardCharsets

import com.typesafe.config.Config
import org.apache.avro.Schema
import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.locationtech.geomesa.convert.avro.AvroConverter._
import org.locationtech.geomesa.convert.avro.AvroConverterFactory.AvroConfigConvert
import org.locationtech.geomesa.convert.Modes.{ErrorMode, ParseMode}
import org.locationtech.geomesa.convert.SimpleFeatureValidator
import org.locationtech.geomesa.convert2.AbstractConverter.{BasicField, BasicOptions}
import org.locationtech.geomesa.convert2.AbstractConverterFactory.{BasicFieldConvert, BasicOptionsConvert, ConverterConfigConvert, ConverterOptionsConvert, FieldConvert, OptionConvert}
import org.locationtech.geomesa.convert2.TypeInference.{FunctionTransform, InferredType}
import org.locationtech.geomesa.convert2.transforms.Expression
import org.locationtech.geomesa.convert2.{AbstractConverterFactory, TypeInference}
import org.locationtech.geomesa.features.avro._
import org.locationtech.geomesa.features.serialization.ObjectType
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.SimpleFeatureType
import pureconfig.ConfigObjectCursor
import pureconfig.error.{ConfigReaderFailures, FailureReason}

import scala.util.control.NonFatal

class AvroConverterFactory extends AbstractConverterFactory[AvroConverter, AvroConfig, BasicField, BasicOptions] {

  import AvroSimpleFeatureUtils.{AVRO_SIMPLE_FEATURE_USERDATA, AVRO_SIMPLE_FEATURE_VERSION, FEATURE_ID_AVRO_FIELD_NAME}
  import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichIterator

  import scala.collection.JavaConverters._

  override protected val typeToProcess: String = "avro"

  override protected implicit def configConvert: ConverterConfigConvert[AvroConfig] = AvroConfigConvert
  override protected implicit def fieldConvert: FieldConvert[BasicField] = BasicFieldConvert
  override protected implicit def optsConvert: ConverterOptionsConvert[BasicOptions] = BasicOptionsConvert

  /**
    * Note: only works on Avro files with embedded schemas
    *
    * @param is input
    * @param sft simple feature type, if known ahead of time
    * @return
    */
  override def infer(is: InputStream, sft: Option[SimpleFeatureType]): Option[(SimpleFeatureType, Config)] = {
    try {
      WithClose(new DataFileStream[GenericRecord](is, new GenericDatumReader[GenericRecord]())) { dfs =>
        val (schema, id, fields, userData) = if (AvroDataFile.canParse(dfs)) {
          // this is a file written in the geomesa avro format
          val records = dfs.iterator.asScala.take(AbstractConverterFactory.inferSampleSize)
          // get the version from the first record
          val version =
            records.headOption
                .flatMap(r => Option(r.get(AVRO_SIMPLE_FEATURE_VERSION)).map(_.asInstanceOf[Int]))
                .getOrElse(AvroSimpleFeatureUtils.VERSION)
          val nameEncoder = new FieldNameEncoder(version)
          val dataSft = AvroDataFile.getSft(dfs)

          val fields = dataSft.getAttributeDescriptors.asScala.map { descriptor =>
            // some types need a function applied to the underlying avro value
            val fn = ObjectType.selectType(descriptor).head match {
              case ObjectType.DATE     => Some("millisToDate")
              case ObjectType.UUID     => Some("avroBinaryUuid")
              case ObjectType.LIST     => Some("avroBinaryList")
              case ObjectType.MAP      => Some("avroBinaryMap")
              case ObjectType.GEOMETRY => Some("geometry") // note: handles both wkt (v1) and wkb (v2)
              case _ => None
            }

            val path = s"avroPath($$1, '/${nameEncoder.encode(descriptor.getLocalName)}')"
            val expression = fn.map(f => s"$f($path)").getOrElse(path)
            BasicField(descriptor.getLocalName, Some(Expression(expression)))
          }

          val id = Expression(s"avroPath($$1, '/$FEATURE_ID_AVRO_FIELD_NAME')")
          val userData: Map[String, Expression] =
            if (dfs.getSchema.getField(AVRO_SIMPLE_FEATURE_USERDATA) == null) { Map.empty } else {
              // avro user data is stored as an array of 'key', 'keyClass', 'value', and 'valueClass'
              // our converters require global key->expression, so pull out the unique keys
              val kvs = scala.collection.mutable.Map.empty[String, Expression]
              records.foreach { record =>
                val ud = record.get(AVRO_SIMPLE_FEATURE_USERDATA).asInstanceOf[java.util.Collection[GenericRecord]]
                ud.asScala.foreach { rec =>
                  Option(rec.get("key")).map(_.toString).foreach { key =>
                    kvs.getOrElseUpdate(key, {
                      var expression = s"avroPath($$1, '/$AVRO_SIMPLE_FEATURE_USERDATA[$$key=$key]/value')"
                      if (Option(rec.get("valueClass")).map(_.toString).contains("java.util.Date")) {
                        // dates have to be converted from millis
                        expression = s"millisToDate($expression)"
                      }
                      Expression(expression)
                    })
                  }
                }
              }
              kvs.toMap
            }

          (dataSft, id, fields, userData)
        } else {
          // this is an arbitrary avro file, create fields based on the schema
          val uniqueNames = scala.collection.mutable.HashSet.empty[String]
          val types = scala.collection.mutable.ArrayBuffer.empty[InferredType]

          def mapField(field: Schema.Field, path: String = ""): Unit = {
            // get a valid attribute name
            val base = s"${field.name().replaceAll("[^A-Za-z0-9]+", "_")}"
            var name = base
            var i = 0
            while (!uniqueNames.add(name)) {
              name = s"${base}_$i"
              i += 1
            }

            // checks for nested array/map types we can handle
            def isSimple: Boolean = field.schema().getFields.asScala.map(_.schema().getType).forall {
              case Schema.Type.STRING  => true
              case Schema.Type.INT     => true
              case Schema.Type.LONG    => true
              case Schema.Type.FLOAT   => true
              case Schema.Type.DOUBLE  => true
              case Schema.Type.BOOLEAN => true
              case _ => false
            }

            val transform = FunctionTransform("avroPath(", s",'$path/${field.name}')")
            field.schema().getType match {
              case Schema.Type.STRING  => types += InferredType(name, ObjectType.STRING, transform)
              case Schema.Type.BYTES   => types += InferredType(name, ObjectType.BYTES, transform)
              case Schema.Type.INT     => types += InferredType(name, ObjectType.INT, transform)
              case Schema.Type.LONG    => types += InferredType(name, ObjectType.LONG, transform)
              case Schema.Type.FLOAT   => types += InferredType(name, ObjectType.FLOAT, transform)
              case Schema.Type.DOUBLE  => types += InferredType(name, ObjectType.DOUBLE, transform)
              case Schema.Type.BOOLEAN => types += InferredType(name, ObjectType.BOOLEAN, transform)
              case Schema.Type.ARRAY   => if (isSimple) { types += InferredType(name, ObjectType.LIST, transform) }
              case Schema.Type.MAP     => if (isSimple) { types += InferredType(name, ObjectType.MAP, transform) }
              case Schema.Type.FIXED   => types += InferredType(name, ObjectType.BYTES, transform)
              case Schema.Type.ENUM    => types += InferredType(name, ObjectType.STRING, transform.copy(suffix = transform.suffix + "::string"))
              case Schema.Type.UNION   => types += InferredType(name, ObjectType.STRING, transform.copy(suffix = transform.suffix + "::string"))
              case Schema.Type.RECORD  => field.schema().getFields.asScala.foreach(mapField(_, s"$path/${field.name}"))
              case _ => // no-op
            }
          }

          dfs.getSchema.getFields.asScala.foreach(mapField(_))

          // check if we can derive a geometry field
          TypeInference.deriveGeometry(types).foreach(g => types += g)

          val dataSft = TypeInference.schema("inferred-avro", types)
          // note: avro values are always stored at index 1
          val id = Expression("md5(string2bytes($1::string))")
          val fields = types.map(t => BasicField(t.name, Some(Expression(t.transform.apply(1)))))

          (dataSft, id, fields, Map.empty[String, Expression])
        }

        // validate the existing schema, if any
        if (sft.exists(_.getAttributeDescriptors.asScala != schema.getAttributeDescriptors.asScala)) {
          throw new IllegalArgumentException("Inferred schema does not match existing schema")
        }

        val converterConfig = AvroConfig(typeToProcess, SchemaEmbedded, Some(id), Map.empty, userData)

        val options =
          BasicOptions(SimpleFeatureValidator.default, ParseMode.Default, ErrorMode(), StandardCharsets.UTF_8)

        val config = configConvert.to(converterConfig)
            .withFallback(fieldConvert.to(fields))
            .withFallback(optsConvert.to(options))
            .toConfig

        Some((schema, config))
      }
    } catch {
      case NonFatal(e) =>
        logger.debug(s"Could not infer Avro converter from input:", e)
        None
    }
  }
}

object AvroConverterFactory {

  object AvroConfigConvert extends ConverterConfigConvert[AvroConfig] with OptionConvert {

    override protected def decodeConfig(cur: ConfigObjectCursor,
                                        `type`: String,
                                        idField: Option[Expression],
                                        caches: Map[String, Config],
                                        userData: Map[String, Expression]): Either[ConfigReaderFailures, AvroConfig] = {

      def schemaOrFile(schema: Option[String],
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
