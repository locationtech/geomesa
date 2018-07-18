/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.json

import java.util.Locale

import com.jayway.jsonpath.JsonPath
import com.typesafe.config.Config
import org.locationtech.geomesa.convert.json.JsonConverter.{JsonField, _}
import org.locationtech.geomesa.convert.json.JsonConverterFactory.{JsonConfigConvert, JsonFieldConvert}
import org.locationtech.geomesa.convert2.AbstractConverter.BasicOptions
import org.locationtech.geomesa.convert2.AbstractConverterFactory
import org.locationtech.geomesa.convert2.AbstractConverterFactory.{BasicOptionsConvert, ConverterConfigConvert, ConverterOptionsConvert, FieldConvert, OptionConvert}
import org.locationtech.geomesa.convert2.transforms.Expression
import pureconfig.ConfigObjectCursor
import pureconfig.error.{CannotConvert, ConfigReaderFailures}

import scala.util.control.NonFatal

class JsonConverterFactory extends AbstractConverterFactory[JsonConverter, JsonConfig, JsonField, BasicOptions] {

  override protected val typeToProcess = "json"

  override protected implicit def configConvert: ConverterConfigConvert[JsonConfig] = JsonConfigConvert
  override protected implicit def fieldConvert: FieldConvert[JsonField] = JsonFieldConvert
  override protected implicit def optsConvert: ConverterOptionsConvert[BasicOptions] = BasicOptionsConvert
}

object JsonConverterFactory {

  object JsonConfigConvert extends ConverterConfigConvert[JsonConfig] with JsonPathConvert {

    override protected def decodeConfig(cur: ConfigObjectCursor,
                                        `type`: String,
                                        idField: Option[Expression],
                                        caches: Map[String, Config],
                                        userData: Map[String, Expression]): Either[ConfigReaderFailures, JsonConfig] = {
      for { path <- pathFrom(cur, "feature-path").right } yield {
        JsonConfig(`type`, path, idField, caches, userData)
      }
    }

    override protected def encodeConfig(config: JsonConfig, base: java.util.Map[String, AnyRef]): Unit =
      config.featurePath.foreach(p => base.put("feature-path", p.toString)) // TODO might need to keep these as strings

  }

  object JsonFieldConvert extends FieldConvert[JsonField] with JsonPathConvert {
    override protected def decodeField(cur: ConfigObjectCursor,
                                       name: String,
                                       transform: Option[Expression]): Either[ConfigReaderFailures, JsonField] = {
      val jsonTypeCur =  cur.atKeyOrUndefined("json-type")
      val jsonType = if (jsonTypeCur.isUndefined) { Right(None) } else { jsonTypeCur.asString.right.map(Option.apply) }

      val config = for {
        jType    <- jsonType.right
        path     <- pathFrom(cur, "path").right
        rootPath <- pathFrom(cur, "root-path").right
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
          base.put(if (f.pathIsRoot) { "root-path" } else { "path" }, f.path.toString)

        case _ => // no-op
      }
    }
  }

  trait JsonPathConvert extends OptionConvert {
    protected def pathFrom(cur: ConfigObjectCursor, key: String): Either[ConfigReaderFailures, Option[JsonPath]] = {
      optional(cur, key).right.flatMap { path =>
        try { Right(path.map(JsonPath.compile(_))) } catch {
          case NonFatal(e) => cur.failed(CannotConvert(cur.value.toString, "JsonPath", e.getMessage))
        }
      }
    }
  }
}