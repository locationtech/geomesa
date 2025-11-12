/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.convert.json

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.gson._
import com.jayway.jsonpath.JsonPath
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert2.transforms.CollectionFunctionFactory.CollectionParsing
import org.locationtech.geomesa.convert2.transforms.TransformerFunction.NamedTransformerFunction
import org.locationtech.geomesa.convert2.transforms.{TransformerFunction, TransformerFunctionFactory}
import org.locationtech.geomesa.utils.text.DateParsing

import java.util.Date
import java.util.concurrent.ConcurrentHashMap

class JsonFunctionFactory extends TransformerFunctionFactory with CollectionParsing with LazyLogging {

  import scala.collection.JavaConverters._

  private val gson = new Gson()
  private val mapper = new ObjectMapper()

  // noinspection ScalaDeprecation
  override def functions: Seq[TransformerFunction] =
    Seq(jsonToString, jsonListParser, jsonMapParser, mapToJson, jsonPath, jsonArrayToObject, newJsonObject, emptyToNull)

  @deprecated("use toString")
  private val jsonToString = new NamedTransformerFunction(Seq("jsonToString", "json2string"), pure = true) {
    override def apply(args: Array[AnyRef]): AnyRef = args(0).toString
    override def withContext(ec: EvaluationContext): TransformerFunction = {
      logger.warn("Using deprecated function 'jsonToString' - use 'toString' instead")
      super.withContext(ec)
    }
  }

  private val jsonPath: TransformerFunction = new NamedTransformerFunction(Array("jsonPath"), pure = true) {
    private val cache = new ConcurrentHashMap[Any, JsonPath]()
    override def apply(args: Array[AnyRef]): AnyRef = {
      var path = cache.get(args(0))
      if (path == null) {
        path = JsonPath.compile(args(0).asInstanceOf[String])
        cache.put(args(0), path)
      }
      val elem = path.read[JsonElement](args(1), JsonConverter.JsonConfiguration)
      // unwrap primitive and null elements
      if (elem.isJsonNull) {
        null
      } else if (elem.isJsonPrimitive) {
        val p = elem.getAsJsonPrimitive
        if (p.isString) {
          p.getAsString
        } else if (p.isNumber) {
          p.getAsNumber
        } else if (p.isBoolean) {
          Boolean.box(p.getAsBoolean)
        } else {
          p.getAsString // this shouldn't really ever happen...
        }
      } else {
        elem
      }
    }
  }

  private val jsonListParser = TransformerFunction.pure("jsonList") { args =>
    val array = args(1).asInstanceOf[JsonArray]
    if (array == null || array.isJsonNull) { null } else {
      val clazz = determineClazz(args(0).asInstanceOf[String])
      val result = new java.util.ArrayList[Any](array.size())
      val iter = array.iterator()
      while (iter.hasNext) {
        val e = iter.next
        if (!e.isJsonNull) {
          result.add(convert(getPrimitive(e.getAsJsonPrimitive), clazz))
        }
      }
      result
    }
  }

  private val jsonMapParser = TransformerFunction.pure("jsonMap") { args =>
    val kClass = determineClazz(args(0).asInstanceOf[String])
    val vClass = determineClazz(args(1).asInstanceOf[String])
    val map = args(2).asInstanceOf[JsonObject]

    if (map == null || map.isJsonNull) { null } else {
      val result = new java.util.HashMap[Any, Any](map.size())
      val iter = map.entrySet().iterator()
      while (iter.hasNext) {
        val e = iter.next
        result.put(convert(e.getKey, kClass), convert(getPrimitive(e.getValue.getAsJsonPrimitive), vClass))
      }
      result
    }
  }

  private val mapToJson = TransformerFunction.pure("map2Json", "mapToJson") { args =>
    mapper.writeValueAsString(args(0).asInstanceOf[java.util.Map[String, _]])
  }

  private val jsonArrayToObject = TransformerFunction.pure("jsonArrayToObject") { args =>
    val array = args(0).asInstanceOf[JsonArray]
    if (array == null || array.isJsonNull) { null } else {
      val obj = new JsonObject()
      var i = 0
      while (i < array.size()) {
        obj.add(Integer.toString(i), array.get(i))
        i += 1
      }
      obj
    }
  }

  private val newJsonObject = TransformerFunction.pure("newJsonObject") { args =>
    val obj = new JsonObject()
    var i = 1
    while (i < args.length) {
      val key = args(i -1).toString
      args(i) match {
        case null => // skip nulls
        case j: JsonElement => obj.add(key, j)
        case j: String  => obj.add(key, new JsonPrimitive(j))
        case j: Number  => obj.add(key, new JsonPrimitive(j))
        case j: Boolean => obj.add(key, new JsonPrimitive(j))
        case j: Date    => obj.add(key, new JsonPrimitive(DateParsing.formatDate(j)))
        case j          => obj.add(key, gson.toJsonTree(j))
      }

      i += 2
    }
    obj
  }

  private val emptyToNull = TransformerFunction.pure("emptyJsonToNull") { args =>
    args(0) match {
      case JsonNull.INSTANCE => null
      case j: JsonObject if j.size() == 0 => null
      case j: JsonObject if j.entrySet().asScala.forall(_.getValue == JsonNull.INSTANCE) => null
      case j: JsonArray if j.size() == 0 => null
      case j => j
    }
  }

  private def getPrimitive(p: JsonPrimitive): Any = if (p.isBoolean) { p.getAsBoolean } else { p.getAsString }
}
