/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.json

import com.google.gson._
import com.jayway.jsonpath.JsonPath
import org.json4s.JsonAST.{JNull, JValue}
import org.json4s.{JBool, JDouble, JInt, JString}
import org.locationtech.geomesa.convert2.transforms.CollectionFunctionFactory.CollectionParsing
import org.locationtech.geomesa.convert2.transforms.TransformerFunction.NamedTransformerFunction
import org.locationtech.geomesa.convert2.transforms.{TransformerFunction, TransformerFunctionFactory}
import org.locationtech.geomesa.utils.text.DateParsing

import java.util.Date
import java.util.concurrent.ConcurrentHashMap

class JsonFunctionFactory extends TransformerFunctionFactory with CollectionParsing {

  import scala.collection.JavaConverters._

  private val gson = new Gson()

  // noinspection ScalaDeprecation
  override def functions: Seq[TransformerFunction] =
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
    Seq(jsonToString, jsonListParser, jsonMapParser, mapToJson, jsonPath, jsonArrayToObject, newJsonObject, emptyToNull)
=======
    Seq(jsonToString, jsonListParser, jsonMapParser, mapToJson, jsonPath, jsonArrayToObject)
>>>>>>> 1e76dbd1e7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
    Seq(jsonToString, jsonListParser, jsonMapParser, mapToJson, jsonPath, jsonArrayToObject, newJsonObject, emptyToNull)
>>>>>>> 1a21a3c300 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 595c43086a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
    Seq(jsonToString, jsonListParser, jsonMapParser, mapToJson, jsonPath, jsonArrayToObject, newJsonObject, emptyToNull)
=======
    Seq(jsonToString, jsonListParser, jsonMapParser, mapToJson, jsonPath, jsonArrayToObject)
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 4aef7a70f3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
    Seq(jsonToString, jsonListParser, jsonMapParser, mapToJson, jsonPath, jsonArrayToObject, newJsonObject, emptyToNull)
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))

  @deprecated("use toString")
  private val jsonToString = TransformerFunction.pure("jsonToString", "json2string") { args =>
    args(0).toString
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

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1a21a3c300 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4aef7a70f3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
  private val newJsonObject = TransformerFunction.pure("newJsonObject") { args =>
    val obj = new JsonObject()
    var i = 1
    while (i < args.length) {
      val key = args(i -1).toString
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> af0a88eb17 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 595c43086a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5ca0cd6de5 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> f7038468b7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 58286bfd3 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5ca0cd6de5 (GEOMESA-3100 Kafka layer views (#2784))
      args(i) match {
        case null => // skip nulls
        case j: JsonElement => obj.add(key, j)
        case j: String  => obj.add(key, new JsonPrimitive(j))
        case j: Number  => obj.add(key, new JsonPrimitive(j))
        case j: Boolean => obj.add(key, new JsonPrimitive(j))
        case j: Date    => obj.add(key, new JsonPrimitive(DateParsing.formatDate(j)))
        case j          => obj.add(key, gson.toJsonTree(j))
<<<<<<< HEAD
<<<<<<< HEAD
      }

=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> 58286bfd3 (GEOMESA-3100 Kafka layer views (#2784))
      }

=======
>>>>>>> 5ca0cd6de5 (GEOMESA-3100 Kafka layer views (#2784))
      val value = args(i) match {
        case null => JsonNull.INSTANCE
        case j: JsonElement => j
        case j: String  => new JsonPrimitive(j)
        case j: Number  => new JsonPrimitive(j)
        case j: Boolean => new JsonPrimitive(j)
        case j: Date    => new JsonPrimitive(DateParsing.formatDate(j))
        case j          => gson.toJsonTree(j)
      }
      obj.add(key, value)
<<<<<<< HEAD
>>>>>>> 1a21a3c300 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
      }

>>>>>>> af0a88eb17 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 595c43086a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
      }

>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> f7038468b7 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
>>>>>>> 58286bfd3 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5ca0cd6de5 (GEOMESA-3100 Kafka layer views (#2784))
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

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1e76dbd1e7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1a21a3c300 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4aef7a70f3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
  private def getPrimitive(p: JsonPrimitive): Any = if (p.isBoolean) { p.getAsBoolean } else { p.getAsString }
}
