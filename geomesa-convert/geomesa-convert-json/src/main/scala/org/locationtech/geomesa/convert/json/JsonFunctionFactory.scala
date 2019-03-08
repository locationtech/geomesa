/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.json

import java.util.concurrent.ConcurrentHashMap

import com.google.gson.{JsonArray, JsonElement, JsonObject, JsonPrimitive}
import com.jayway.jsonpath.JsonPath
import org.json4s.JsonAST.{JNull, JValue}
import org.json4s.{JBool, JDouble, JInt, JString}
import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert2.transforms.CollectionFunctionFactory.CollectionParsing
import org.locationtech.geomesa.convert2.transforms.TransformerFunction.NamedTransformerFunction
import org.locationtech.geomesa.convert2.transforms.{TransformerFunction, TransformerFunctionFactory}

class JsonFunctionFactory extends TransformerFunctionFactory with CollectionParsing {

  import scala.collection.JavaConverters._

  // noinspection ScalaDeprecation
  override def functions: Seq[TransformerFunction] =
    Seq(jsonToString, jsonListParser, jsonMapParser, mapToJson, jsonPath)

  @deprecated("use toString")
  private val jsonToString = TransformerFunction.pure("jsonToString", "json2string") { args =>
    args(0).toString
  }

  private val jsonPath: TransformerFunction = new NamedTransformerFunction(Array("jsonPath"), pure = true) {
    private val cache = new ConcurrentHashMap[Any, JsonPath]()
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = {
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
    if (array.isJsonNull) { null } else {
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

    if (map.isJsonNull) { null } else {
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

    import org.json4s.JsonDSL._
    import org.json4s.native.JsonMethods._

    val map = args(0).asInstanceOf[java.util.Map[String, _]]
    val ast: Map[String, JValue] = map.asScala.mapValues {
      case null       => JNull
      case x: Int     => JInt(x)
      case x: Long    => JInt(x)
      case x: Double  => JDouble(x)
      case x: Float   => JDouble(x.toDouble)
      case x: Boolean => JBool(x)
      case x: String  => JString(x)
      case x          => JString(x.toString)
    }.toMap
    compact(render(ast))
  }

  private def getPrimitive(p: JsonPrimitive): Any = if (p.isBoolean) { p.getAsBoolean } else { p.getAsString }
}
