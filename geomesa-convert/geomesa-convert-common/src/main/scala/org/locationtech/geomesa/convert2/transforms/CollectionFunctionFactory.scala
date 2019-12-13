/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2.transforms

import java.util.{Collections, UUID}

import org.locationtech.geomesa.convert2.transforms.CollectionFunctionFactory.CollectionParsing
import org.locationtech.geomesa.utils.geotools.converters.FastConverter

class CollectionFunctionFactory extends TransformerFunctionFactory with CollectionParsing {

  import scala.collection.JavaConverters._

  override def functions: Seq[TransformerFunction] = Seq(listParserFn, mapParserFn, listFn, mapValueFunction)

  private val defaultListDelim = ","
  private val defaultKVDelim   = "->"

  private val listParserFn = TransformerFunction.pure("parseList") { args =>
    val clazz = determineClazz(args(0).asInstanceOf[String])
    val s = args(1).asInstanceOf[String]
    val delim = if (args.length >= 3) args(2).asInstanceOf[String] else defaultListDelim

    if (s.isEmpty) {
      Collections.emptyList()
    } else {
      s.split(delim).map(_.trim).map(convert(_, clazz)).toList.asJava
    }
  }

  private val mapParserFn = TransformerFunction.pure("parseMap") { args =>
    val kv = args(0).asInstanceOf[String].split("->").map(_.trim)
    val keyClazz = determineClazz(kv(0))
    val valueClazz = determineClazz(kv(1))
    val s: String = args(1).toString
    val kvDelim: String = if (args.length >= 3) args(2).asInstanceOf[String] else defaultKVDelim
    val pairDelim: String = if (args.length >= 4) args(3).asInstanceOf[String] else defaultListDelim

    if (s.isEmpty) {
      Collections.emptyMap()
    } else {
      s.split(pairDelim)
          .map(_.split(kvDelim).map(_.trim))
          .map { case Array(key, value) =>
            (convert(key, keyClazz), convert(value, valueClazz))
          }.toMap.asJava
    }
  }

  private val listFn = TransformerFunction.pure("list") { args =>
    args.toList.asJava
  }

  private val mapValueFunction = TransformerFunction.pure("mapValue") {
    args => args(0).asInstanceOf[java.util.Map[Any, Any]].get(args(1))
  }

}

object CollectionFunctionFactory {

  trait CollectionParsing {

    protected def convert(value: Any, clazz: Class[_]): Any = {
      val converted = FastConverter.convert(value, clazz)
      if (converted == null) {
        throw new IllegalArgumentException(s"Could not convert value '$value' to type ${clazz.getName})")
      }
      converted
    }

    protected def determineClazz(s: String): Class[_] = s.toLowerCase match {
      case "string" | "str"   => classOf[String]
      case "int" | "integer"  => classOf[java.lang.Integer]
      case "long"             => classOf[java.lang.Long]
      case "double"           => classOf[java.lang.Double]
      case "float"            => classOf[java.lang.Float]
      case "bool" | "boolean" => classOf[java.lang.Boolean]
      case "bytes"            => classOf[Array[Byte]]
      case "uuid"             => classOf[UUID]
      case "date"             => classOf[java.util.Date]
    }
  }
}
