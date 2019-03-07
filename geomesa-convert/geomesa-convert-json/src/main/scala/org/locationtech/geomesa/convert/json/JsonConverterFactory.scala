/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.json

import java.io.{InputStream, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.util.Locale

import com.google.gson.stream.{JsonReader, JsonToken}
import com.google.gson.{JsonElement, JsonParser}
import com.typesafe.config.Config
import org.locationtech.geomesa.convert.json.GeoJsonParsing.GeoJsonFeature
import org.locationtech.geomesa.convert.json.JsonConverter.{JsonField, _}
import org.locationtech.geomesa.convert.json.JsonConverterFactory.{JsonConfigConvert, JsonFieldConvert}
import org.locationtech.geomesa.convert.Modes.{ErrorMode, ParseMode}
import org.locationtech.geomesa.convert.SimpleFeatureValidator
import org.locationtech.geomesa.convert2.AbstractConverter.BasicOptions
import org.locationtech.geomesa.convert2.AbstractConverterFactory.{BasicOptionsConvert, ConverterConfigConvert, ConverterOptionsConvert, FieldConvert, OptionConvert}
import org.locationtech.geomesa.convert2.TypeInference.{IdentityTransform, InferredType}
import org.locationtech.geomesa.convert2.transforms.Expression
import org.locationtech.geomesa.convert2.{AbstractConverterFactory, TypeInference}
import org.locationtech.geomesa.features.serialization.ObjectType
import org.locationtech.geomesa.features.serialization.ObjectType.ObjectType
import org.locationtech.geomesa.utils.geotools.FeatureUtils
import org.opengis.feature.simple.SimpleFeatureType
import pureconfig.ConfigObjectCursor
import pureconfig.error.{CannotConvert, ConfigReaderFailures}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.control.NonFatal

class JsonConverterFactory extends AbstractConverterFactory[JsonConverter, JsonConfig, JsonField, BasicOptions] {

  override protected val typeToProcess: String = JsonConverterFactory.TypeToProcess

  override protected implicit def configConvert: ConverterConfigConvert[JsonConfig] = JsonConfigConvert
  override protected implicit def fieldConvert: FieldConvert[JsonField] = JsonFieldConvert
  override protected implicit def optsConvert: ConverterOptionsConvert[BasicOptions] = BasicOptionsConvert

  override def infer(is: InputStream, sft: Option[SimpleFeatureType]): Option[(SimpleFeatureType, Config)] = {
    try {
      val reader = new JsonReader(new InputStreamReader(is, StandardCharsets.UTF_8))
      reader.setLenient(true)

      val elements = {
        val iter: Iterator[JsonElement] = new Iterator[JsonElement] {
          private val parser = new JsonParser
          override def hasNext: Boolean = reader.peek() != JsonToken.END_DOCUMENT
          override def next(): JsonElement = parser.parse(reader)
        }
        iter.take(AbstractConverterFactory.inferSampleSize).toSeq
      }

      val geojson = elements.collect {
        case el if JsonConverter.isFeature(el) => JsonConverter.parseFeature(el)
        case el if JsonConverter.isFeatureCollection(el) => JsonConverter.parseFeatureCollection(el)
      }

      geojson.headOption.map { head =>
        // is this a feature collection or individual features
        val featurePath = head match {
          case _: GeoJsonFeature => None
          case _: Seq[GeoJsonFeature] => Some("$.features[*]")
        }
        val idField = Some(Expression("md5(string2bytes(json2string($0)))"))

        // flatten out any feature collections into features
        val features = geojson.flatMap {
          case g: GeoJsonFeature => Seq(g)
          case g: Seq[GeoJsonFeature] => g
        }

        // track the 'properties' and geometry type in each feature
        val props = scala.collection.mutable.Map.empty[String, ListBuffer[String]]
        val geoms = scala.collection.mutable.Set.empty[ObjectType]

        features.take(AbstractConverterFactory.inferSampleSize).foreach { feature =>
          geoms += TypeInference.infer(Seq(Seq(feature.geom))).head.typed
          feature.properties.foreach { case (k, v) => props.getOrElseUpdate(k, ListBuffer.empty) += v }
        }

        // track the names we use for each column to ensure no duplicates
        val uniqueNames = scala.collection.mutable.HashSet.empty[String]

        // get a valid attribute name based on the json path
        def name(path: String): String = {
          val base = path.replaceFirst("properties", "").replaceAll("[^A-Za-z0-9]+", "_").replaceAll("^_|_$", "")
          var candidate = base
          var i = 0
          while (FeatureUtils.ReservedWords.contains(candidate.toUpperCase(Locale.US)) || !uniqueNames.add(candidate)) {
            candidate = s"${base}_$i"
            i += 1
          }
          candidate
        }

        // track the inferred types of 'properties' entries
        val inferredTypes = ArrayBuffer[InferredType]()

        // field definitions - call .toSeq first to ensure consistent ordering with types
        val fields = props.toSeq.map { case (path, values) =>
          val attr = name(path)
          val inferred = TypeInference.infer(values.map(Seq(_))).head
          inferredTypes += inferred.copy(name = attr) // note: side-effect in map
          // account for optional nodes by wrapping transform with a try/null
          val transform = Some(Expression(s"try(${inferred.transform.apply(0)},null)"))
          new StringJsonField(attr, path, false, transform)
        }

        // the geometry field
        val geomType = if (geoms.size > 1) { ObjectType.GEOMETRY } else { geoms.head }
        val geomField = new GeometryJsonField(name("geom"), "$.geometry", false, None)
        inferredTypes += InferredType(geomField.name, geomType, IdentityTransform)

        // validate the existing schema, if any
        sft.foreach(AbstractConverterFactory.validateInferredType(_, inferredTypes.map(_.typed)))

        val schema = sft.getOrElse(TypeInference.schema("inferred-json", inferredTypes))

        val jsonConfig = JsonConfig(typeToProcess, featurePath, idField, Map.empty, Map.empty)
        val fieldConfig = fields :+ geomField
        val options =
          BasicOptions(SimpleFeatureValidator.default, ParseMode.Default, ErrorMode(), StandardCharsets.UTF_8)

        val config = configConvert.to(jsonConfig)
            .withFallback(fieldConvert.to(fieldConfig))
            .withFallback(optsConvert.to(options))
            .toConfig

        (schema, config)
      }
    } catch {
      case NonFatal(e) =>
        logger.debug(s"Could not infer JSON converter from input:", e)
        None
    }
  }
}

object JsonConverterFactory {

  val TypeToProcess = "json"

  object JsonConfigConvert extends ConverterConfigConvert[JsonConfig] with OptionConvert {

    override protected def decodeConfig(cur: ConfigObjectCursor,
                                        `type`: String,
                                        idField: Option[Expression],
                                        caches: Map[String, Config],
                                        userData: Map[String, Expression]): Either[ConfigReaderFailures, JsonConfig] = {
      for { path <- optional(cur, "feature-path").right } yield {
        JsonConfig(`type`, path, idField, caches, userData)
      }
    }

    override protected def encodeConfig(config: JsonConfig, base: java.util.Map[String, AnyRef]): Unit =
      config.featurePath.foreach(p => base.put("feature-path", p))

  }

  object JsonFieldConvert extends FieldConvert[JsonField] with OptionConvert {
    override protected def decodeField(cur: ConfigObjectCursor,
                                       name: String,
                                       transform: Option[Expression]): Either[ConfigReaderFailures, JsonField] = {
      val jsonTypeCur =  cur.atKeyOrUndefined("json-type")
      val jsonType = if (jsonTypeCur.isUndefined) { Right(None) } else { jsonTypeCur.asString.right.map(Option.apply) }

      val config = for {
        jType    <- jsonType.right
        path     <- optional(cur, "path").right
        rootPath <- optional(cur, "root-path").right
      } yield {
        (jType, path, rootPath)
      }
      config.right.flatMap { case (jType, path, rootPath) =>
        if (path.isDefined && rootPath.isDefined) {
          cur.failed(CannotConvert(cur.value.toString, "JsonField", "Json fields must define only one of 'path' or 'root-path'"))
        } else if (jType.isDefined && path.isEmpty && rootPath.isEmpty) {
          cur.failed(CannotConvert(cur.value.toString, "JsonField", "Json fields must define a 'path' or 'root-path'"))
        } else if (jType.isEmpty) {
          Right(DerivedField(name, transform))
        } else {
          val (jsonPath, pathIsRoot) = (path, rootPath) match {
            case (Some(p), None) => (p, false)
            case (None, Some(p)) => (p, true)
          }
          jType.get.toLowerCase(Locale.US) match {
            case "string"           => Right(new StringJsonField(name, jsonPath, pathIsRoot, transform))
            case "float"            => Right(new FloatJsonField(name, jsonPath, pathIsRoot, transform))
            case "double"           => Right(new DoubleJsonField(name, jsonPath, pathIsRoot, transform))
            case "integer" | "int"  => Right(new IntJsonField(name, jsonPath, pathIsRoot, transform))
            case "boolean" | "bool" => Right(new BooleanJsonField(name, jsonPath, pathIsRoot, transform))
            case "long"             => Right(new LongJsonField(name, jsonPath, pathIsRoot, transform))
            case "geometry"         => Right(new GeometryJsonField(name, jsonPath, pathIsRoot, transform))
            case "array" | "list"   => Right(new ArrayJsonField(name, jsonPath, pathIsRoot, transform))
            case "object" | "map"   => Right(new ObjectJsonField(name, jsonPath, pathIsRoot, transform))
            case t => cur.failed(CannotConvert(cur.value.toString, "JsonField", s"Invalid json-type '$t'"))
          }
        }
      }
    }

    override protected def encodeField(field: JsonField, base: java.util.Map[String, AnyRef]): Unit = {
      field match {
        case f: TypedJsonField =>
          base.put("json-type", f.jsonType)
          base.put(if (f.pathIsRoot) { "root-path" } else { "path" }, f.path)

        case _ => // no-op
      }
    }
  }
}
