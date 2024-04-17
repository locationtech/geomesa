/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.json

import com.google.gson.stream.{JsonReader, JsonToken}
import com.google.gson.{JsonElement, JsonNull, JsonParser}
import com.jayway.jsonpath.{JsonPath, PathNotFoundException}
import com.typesafe.config.Config
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert.json.GeoJsonParsing.GeoJsonFeature
import org.locationtech.geomesa.convert.json.JsonConverter._
import org.locationtech.geomesa.convert.json.JsonConverterFactory.{JsonConfigConvert, JsonFieldConvert, PropNamer}
import org.locationtech.geomesa.convert2.AbstractConverter.BasicOptions
import org.locationtech.geomesa.convert2.AbstractConverterFactory.{BasicOptionsConvert, ConverterConfigConvert, FieldConvert, OptionConvert}
import org.locationtech.geomesa.convert2.TypeInference.{IdentityTransform, Namer, PathWithValues, TypeWithPath}
import org.locationtech.geomesa.convert2.transforms.Expression
import org.locationtech.geomesa.convert2.{AbstractConverterFactory, TypeInference}
import org.locationtech.geomesa.utils.geotools.ObjectType
import org.locationtech.geomesa.utils.io.WithClose
import pureconfig.ConfigObjectCursor
import pureconfig.error.{CannotConvert, ConfigReaderFailures}

import java.io.{InputStream, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.util.Locale
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Try}

class JsonConverterFactory extends AbstractConverterFactory[JsonConverter, JsonConfig, JsonField, BasicOptions](
  JsonConverterFactory.TypeToProcess, JsonConfigConvert, JsonFieldConvert, BasicOptionsConvert) {

  import scala.collection.JavaConverters._

  override def infer(
      is: InputStream,
      sft: Option[SimpleFeatureType],
      path: Option[String]): Option[(SimpleFeatureType, Config)] =
    infer(is, sft, path.map(EvaluationContext.inputFileParam).getOrElse(Map.empty)).toOption

  /**
   * Infer a configuration and simple feature type from an input stream, if possible
   *
   * Available hints:
   *  - `featurePath` - json path expression pointing to the feature element
   *
   * @param is input
   * @param sft simple feature type, if known ahead of time
   * @param hints implementation specific hints about the input
   * @return
   */
  override def infer(
      is: InputStream,
      sft: Option[SimpleFeatureType],
      hints: Map[String, AnyRef]): Try[(SimpleFeatureType, Config)] = {

    val tryElements = Try {
      WithClose(new JsonReader(new InputStreamReader(is, StandardCharsets.UTF_8))) { reader =>
        reader.setLenient(true)
        val iter: Iterator[JsonElement] = new Iterator[JsonElement] {
          override def hasNext: Boolean = reader.peek() != JsonToken.END_DOCUMENT
          override def next(): JsonElement = JsonParser.parseReader(reader)
        }
        iter.take(AbstractConverterFactory.inferSampleSize).toList
      }
    }
    tryElements.flatMap { elements =>
      lazy val featurePath = hints.get(JsonConverterFactory.FeaturePathKey).map(_.toString)
      if (elements.isEmpty) {
        Failure(new RuntimeException("Could not parse the input as JSON"))
      } else if (JsonConverter.isFeature(elements.head)) {
        Try(elements.map(JsonConverter.parseFeature))
            .flatMap(inferGeoJson(_, None, sft))
      } else if (JsonConverter.isFeatureCollection(elements.head)) {
        Try(elements.flatMap(JsonConverter.parseFeatureCollection))
            .flatMap(inferGeoJson(_, Some("$.features[*]"), sft))
      } else if (elements.head.isJsonObject) {
        inferJson(elements, featurePath, sft)
      } else if (elements.head.isJsonArray) {
        inferJson(elements, featurePath.orElse(Some("$.[*]")), sft)
      } else {
        Failure(new RuntimeException("Could not parse the input as a JSON object or array"))
      }
    }
  }

  private def inferGeoJson(
      features: Seq[GeoJsonFeature],
      featurePath: Option[String],
      sft: Option[SimpleFeatureType]): Try[(SimpleFeatureType, Config)] = Try {
    // track the 'properties', geometry type and 'id' in each feature
    // use linkedHashMap to retain insertion order
    val props = scala.collection.mutable.LinkedHashMap.empty[String, ListBuffer[Any]]
    val geoms = ListBuffer.empty[Any]
    var hasId = true

    features.take(AbstractConverterFactory.inferSampleSize).foreach { feature =>
      feature.properties.foreach { case (k, v) => props.getOrElseUpdate(k, ListBuffer.empty) += v }
      geoms += feature.geom
      hasId = hasId && feature.id.isDefined
    }

    val pathsAndValues =
      props.toSeq.map { case (path, values) => PathWithValues(path, values) } :+
          PathWithValues(JsonConverterFactory.GeoJsonGeometryPath, geoms)
    val inferredTypes = TypeInference.infer(pathsAndValues, sft.toRight("inferred-json"), new PropNamer())

    val idJsonField = if (hasId) { Some(StringJsonField("id", "$.id", pathIsRoot = false, None)) } else { None }
    val idField = idJsonField match {
      case None    => Some(Expression("md5(stringToBytes(toString($0)))"))
      case Some(f) => Some(Expression(s"$$${f.name}"))
    }
    val fieldConfig = idJsonField.toSeq ++ inferredTypes.types.map(createFieldConfig)

    val jsonConfig = JsonConfig(typeToProcess, featurePath, idField, Map.empty, Map.empty)

    val config = configConvert.to(jsonConfig)
        .withFallback(fieldConvert.to(fieldConfig))
        .withFallback(optsConvert.to(BasicOptions.default))
        .toConfig

    (inferredTypes.sft, config)
  }

  private def inferJson(
      elements: Seq[JsonElement],
      featurePath: Option[String],
      sft: Option[SimpleFeatureType]): Try[(SimpleFeatureType, Config)] = {
    val tryFeatures = Try {
      val features = featurePath match {
        case None => elements
        case Some(p) =>
          val path = JsonPath.compile(p)
          elements.flatMap { el =>
            val res = try { path.read[JsonElement](el, JsonConverter.JsonConfiguration) } catch {
              case _: PathNotFoundException => JsonNull.INSTANCE
            }
            if (res.isJsonArray) {
              res.getAsJsonArray.asList().asScala
            } else {
              Seq(res)
            }
          }
      }
      features.collect { case r if r.isJsonObject => r.getAsJsonObject }
    }

    tryFeatures.flatMap { features =>
      if (features.isEmpty) {
        Failure(new RuntimeException("Could not parse input as JSON"))
      } else {
        Try {
          // track the properties in each feature
          // use linkedHashMap to retain insertion order
          val props = scala.collection.mutable.LinkedHashMap.empty[String, ListBuffer[Any]]

          features.take(AbstractConverterFactory.inferSampleSize).foreach { feature =>
            GeoJsonParsing.parseElement(feature, "").foreach { case (k, v) =>
              props.getOrElseUpdate(k, ListBuffer.empty) += v
            }
          }

          val pathsAndValues = props.toSeq.map { case (path, values) => PathWithValues(path, values) }
          val inferredTypes = TypeInference.infer(pathsAndValues, sft.toRight("inferred-json"))

          val idField = Some(Expression("md5(stringToBytes(toString($0)))"))
          val fieldConfig = inferredTypes.types.map(createFieldConfig)

          val jsonConfig = JsonConfig(typeToProcess, featurePath, idField, Map.empty, Map.empty)

          val config =
            configConvert.to(jsonConfig)
                .withFallback(fieldConvert.to(fieldConfig))
                .withFallback(optsConvert.to(BasicOptions.default))
                .toConfig

          (inferredTypes.sft, config)
        }
      }
    }
  }

  private def createFieldConfig(typed: TypeWithPath): JsonField = {
    val TypeWithPath(path, inferredType) = typed
    val transform = inferredType.transform match {
      case IdentityTransform => None
      // account for optional nodes by wrapping transform with a try/null
      case t => Some(Expression(s"try(${t.apply(0)},null)"))
    }
    if (path.isEmpty) {
      DerivedField(inferredType.name, transform)
    } else if (path == JsonConverterFactory.GeoJsonGeometryPath) {
      GeometryJsonField(inferredType.name, path, pathIsRoot = false, None)
    } else {
      inferredType.typed match {
        case ObjectType.BOOLEAN =>
          BooleanJsonField(inferredType.name, path, pathIsRoot = false, transform)
        case ObjectType.LIST =>
          // if type is list, that means the transform is 'identity', but we need to replace it with jsonList.
          // this is due to GeoJsonParsing decoding the json array for us, above
          ArrayJsonField(inferredType.name, path, pathIsRoot = false, Some(Expression("try(jsonList('string',$0),null)")))
        case _ =>
          // all other types will be parsed as strings with appropriate transforms
          StringJsonField(inferredType.name, path, pathIsRoot = false, transform)
      }
    }
  }
}

object JsonConverterFactory {

  val TypeToProcess = "json"

  val FeaturePathKey = "featurePath"

  private val GeoJsonGeometryPath = "$.geometry"

  object JsonConfigConvert extends ConverterConfigConvert[JsonConfig] with OptionConvert {

    override protected def decodeConfig(
        cur: ConfigObjectCursor,
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
          cur.failed(CannotConvert(cur.toString, "JsonField", "Json fields must define only one of 'path' or 'root-path'"))
        } else if (jType.isDefined && path.isEmpty && rootPath.isEmpty) {
          cur.failed(CannotConvert(cur.toString, "JsonField", "Json fields must define a 'path' or 'root-path'"))
        } else if (jType.isEmpty) {
          Right(DerivedField(name, transform))
        } else {
          val (jsonPath, pathIsRoot) = (path, rootPath) match {
            case (Some(p), None) => (p, false)
            case (None, Some(p)) => (p, true)
          }
          jType.get.toLowerCase(Locale.US) match {
            case "string"           => Right(StringJsonField(name, jsonPath, pathIsRoot, transform))
            case "float"            => Right(FloatJsonField(name, jsonPath, pathIsRoot, transform))
            case "double"           => Right(DoubleJsonField(name, jsonPath, pathIsRoot, transform))
            case "integer" | "int"  => Right(IntJsonField(name, jsonPath, pathIsRoot, transform))
            case "boolean" | "bool" => Right(BooleanJsonField(name, jsonPath, pathIsRoot, transform))
            case "long"             => Right(LongJsonField(name, jsonPath, pathIsRoot, transform))
            case "geometry"         => Right(GeometryJsonField(name, jsonPath, pathIsRoot, transform))
            case "array" | "list"   => Right(ArrayJsonField(name, jsonPath, pathIsRoot, transform))
            case "object" | "map"   => Right(ObjectJsonField(name, jsonPath, pathIsRoot, transform))
            case t => cur.failed(CannotConvert(cur.toString, "JsonField", s"Invalid json-type '$t'"))
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

  private class PropNamer extends Namer {
    override def apply(key: String): String =
      super.apply(if (key == GeoJsonGeometryPath) { "geom" } else { key.replaceFirst("properties", "") })
  }
}
