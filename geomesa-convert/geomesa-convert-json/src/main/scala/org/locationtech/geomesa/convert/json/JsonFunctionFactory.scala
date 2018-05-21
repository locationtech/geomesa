/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.json

import com.google.gson.{JsonArray, JsonElement, JsonObject, JsonPrimitive}
import org.geotools.util.Converters
import org.json4s.JsonAST.{JNull, JValue}
import org.json4s.{JBool, JDouble, JInt, JString}
import org.locationtech.geomesa.convert.{EvaluationContext, MapListParsing, TransformerFn, TransformerFunctionFactory}

class JsonFunctionFactory extends TransformerFunctionFactory {
  private val json2string = new TransformerFn {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any =
      args(0).asInstanceOf[JsonElement].toString

    override val names = Seq("json2string", "jsonToString")
  }

  override val functions: Seq[TransformerFn] = Seq(json2string)
}

class JsonMapListFunctionFactory extends TransformerFunctionFactory with MapListParsing {
  override def functions = Seq(jsonListParser, jsonMapParser, mapToJson)

  import scala.collection.JavaConverters._

  private def getPrimitive(p: JsonPrimitive): Any = {
    if (p.isBoolean) {
      p.getAsBoolean
    } else if (p.isNumber) {
      p.getAsString
    } else {
      p.getAsString
    }
  }

  private def convert(value: Any, clazz: Class[_]): Any =
    Option(Converters.convert(value, clazz))
      .getOrElse(throw new IllegalArgumentException(s"Could not convert value  '$value' to type ${clazz.getName})"))

  private val jsonListParser = TransformerFn("jsonList") { args =>
    val clazz = determineClazz(args(0).asInstanceOf[String])
    val jArr = args(1).asInstanceOf[JsonArray]

    if (jArr.isJsonNull) {
      null
    } else {
      import scala.collection.JavaConversions._
      jArr.iterator.filterNot(_.isJsonNull).map(p => convert(getPrimitive(p.getAsJsonPrimitive), clazz)).toList.asJava
    }
  }

  private val jsonMapParser = TransformerFn("jsonMap") { args =>
    val kClass = determineClazz(args(0).asInstanceOf[String])
    val vClass = determineClazz(args(1).asInstanceOf[String])
    val jMap = args(2).asInstanceOf[JsonObject]

    if (jMap.isJsonNull) {
      null
    } else {
      import scala.collection.JavaConversions._
      jMap.entrySet.map { e =>
        val k = convert(e.getKey, kClass)
        val v = convert(getPrimitive(e.getValue.getAsJsonPrimitive), vClass)
        k -> v
      }.toMap.asJava
    }
  }

  private val mapToJson = new TransformerFn {
    override val names = Seq("map2Json", "mapToJson")

    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = {
      import org.json4s.JsonDSL._
      import org.json4s.native.JsonMethods._

      import scala.collection.JavaConversions._

      val map = args(0).asInstanceOf[java.util.Map[String, _]]
      val ast: Map[String, JValue] = map.mapValues {
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
  }
}
