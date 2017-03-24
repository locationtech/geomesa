/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.json

import com.google.gson.{JsonArray, JsonElement, JsonObject, JsonPrimitive}
import org.geotools.util.Converters
import org.locationtech.geomesa.convert.Transformers.EvaluationContext
import org.locationtech.geomesa.convert.{MapListParsing, TransformerFn, TransformerFunctionFactory}

class JsonFunctionFactory extends TransformerFunctionFactory {
  private val json2string = new TransformerFn {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any =
      args(0).asInstanceOf[JsonElement].toString

    override val names = Seq("json2string", "jsonToString")
  }

  override val functions: Seq[TransformerFn] = Seq(json2string)
}

class JsonMapListFunctionFactory extends TransformerFunctionFactory with MapListParsing {
  override def functions = Seq(jsonListParser, jsonMapParser)

  import scala.collection.JavaConverters._

  def getPrimitive(p: JsonPrimitive) = {
    if (p.isBoolean) {
      p.getAsBoolean
    } else if (p.isNumber) {
      p.getAsString
    } else {
      p.getAsString
    }
  }

  def convert(value: Any, clazz: Class[_]) =
    Option(Converters.convert(value, clazz))
      .getOrElse(throw new IllegalArgumentException(s"Could not convert value  '$value' to type ${clazz.getName})"))

  val jsonListParser = TransformerFn("jsonList") { args =>
    val clazz = determineClazz(args(0).asInstanceOf[String])
    val jArr = args(1).asInstanceOf[JsonArray]

    if (jArr.isJsonNull) {
      null
    } else {
      import scala.collection.JavaConversions._
      jArr.iterator.toSeq.map(p => convert(getPrimitive(p.getAsJsonPrimitive), clazz)).toList.asJava
    }
  }

  val jsonMapParser = TransformerFn("jsonMap") { args =>
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

}
