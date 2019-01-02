/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.json

import com.google.gson.{JsonArray, JsonElement, JsonObject, JsonPrimitive}
import org.json4s.JsonAST.{JNull, JValue}
import org.json4s.{JBool, JDouble, JInt, JString}
import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert2.transforms.CollectionFunctionFactory.CollectionParsing
import org.locationtech.geomesa.convert2.transforms.{TransformerFunction, TransformerFunctionFactory}

class JsonFunctionFactory extends TransformerFunctionFactory with CollectionParsing {

  import scala.collection.JavaConverters._

  override def functions: Seq[TransformerFunction] = Seq(jsonToString, jsonListParser, jsonMapParser, mapToJson)

  private val jsonToString = TransformerFunction("jsonToString", "json2string") {
    args => args(0).asInstanceOf[JsonElement].toString
  }

  private val jsonListParser = TransformerFunction("jsonList") { args =>
    val clazz = determineClazz(args(0).asInstanceOf[String])
    val jArr = args(1).asInstanceOf[JsonArray]

    if (jArr.isJsonNull) {
      null
    } else {
      import scala.collection.JavaConversions._
      jArr.iterator.filterNot(_.isJsonNull).map(p => convert(getPrimitive(p.getAsJsonPrimitive), clazz)).toList.asJava
    }
  }

  private val jsonMapParser = TransformerFunction("jsonMap") { args =>
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

  private val mapToJson = new TransformerFunction {
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

  private def getPrimitive(p: JsonPrimitive): Any = if (p.isBoolean) { p.getAsBoolean } else { p.getAsString }
}
