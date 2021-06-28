/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2.transforms

<<<<<<< HEAD
=======
import java.util.{Collections, UUID}

>>>>>>> 51a90e7f04 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert2.transforms.CollectionFunctionFactory.{CollectionParsing, TransformList}
import org.locationtech.geomesa.convert2.transforms.Expression.LiteralString
import org.locationtech.geomesa.convert2.transforms.TransformerFunction.NamedTransformerFunction
import org.locationtech.geomesa.utils.geotools.converters.FastConverter

import java.util.{Collections, UUID}

class CollectionFunctionFactory extends TransformerFunctionFactory with CollectionParsing {

  import scala.collection.JavaConverters._

  override def functions: Seq[TransformerFunction] =
    Seq(listParserFn, mapParserFn, listFn, mapValueFunction, transformList, listItem)

  private val defaultListDelim = ","
  private val defaultKVDelim   = "->"

  private val listParserFn = TransformerFunction.pure("parseList") { args =>
    args(1) match {
      case s: String if s.nonEmpty =>
        val clazz = determineClazz(args(0).asInstanceOf[String])
        val delim = if (args.length >= 3) { args(2).asInstanceOf[String] } else { defaultListDelim }
        s.split(delim).map(i => convert(i.trim(), clazz)).toList.asJava

      case "" => Collections.emptyList()

      case null => null

      case s => throw new IllegalArgumentException(s"Expected a String but got $s:${s.getClass.getName}")
    }
  }

  private val mapParserFn = TransformerFunction.pure("parseMap") { args =>
    args(1) match {
      case s: String if s.nonEmpty =>
        val types = args(0).asInstanceOf[String].split("->").map(_.trim)
        val keyClazz = determineClazz(types(0))
        val valueClazz = determineClazz(types(1))
        val kvDelim = if (args.length >= 3) { args(2).asInstanceOf[String] } else { defaultKVDelim }
        val pairDelim = if (args.length >= 4) { args(3).asInstanceOf[String] } else { defaultListDelim }
        val pairs = s.split(pairDelim).map { pair =>
          pair.split(kvDelim) match {
            case Array(key, value) => (convert(key.trim(), keyClazz), convert(value.trim(), valueClazz))
            case _ => throw new IllegalArgumentException(s"Unexpected key/value pair: $pair")
          }
        }
        pairs.toMap.asJava

      case "" => Collections.emptyMap()

      case null => null

      case s => throw new IllegalArgumentException(s"Expected a String but got $s:${s.getClass.getName}")
    }
  }

  private val listFn = TransformerFunction.pure("list") { args =>
    args.toList.asJava
  }

  private val mapValueFunction = TransformerFunction.pure("mapValue") { args =>
    args(0) match {
      case m: java.util.Map[Any, Any] => m.get(args(1))
      case null => null
      case m => throw new IllegalArgumentException(s"Expected a java.util.Map but got $m:${m.getClass.getName}")
    }
  }
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 51a90e7f04 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a9f01eaaf5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
  }
<<<<<<< HEAD
=======
>>>>>>> cdb4102515 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 92dccb4d7a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 30d431265a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 7e68948ac6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 30d431265a (GEOMESA-3092 Support Lambda NiFi processor (#2777))

  private val listItem = TransformerFunction.pure("listItem") { args =>
    args(0) match {
      case list: java.util.List[Any] => list.get(args(1).asInstanceOf[Int])
      case null => null
      case list => throw new IllegalArgumentException(s"Expected a java.util.List but got $list:${list.getClass.getName}")
    }
  }

  private val transformList = new TransformList(null)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
  }
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f71fa3c0e1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9cdd96d1c0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))

  private val listItem = TransformerFunction.pure("listItem") { args =>
    args(0) match {
      case list: java.util.List[Any] => list.get(args(1).asInstanceOf[Int])
      case null => null
      case list => throw new IllegalArgumentException(s"Expected a java.util.List but got $list:${list.getClass.getName}")
    }
  }

  private val transformList = new TransformList(null)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 51a90e7f04 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> a9f01eaaf5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9cdd96d1c0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> locationtech-main
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> b1c506839 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f71fa3c0e1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> cdb4102515 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 92dccb4d7a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 30d431265a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 7e68948ac6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 30d431265a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
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

  private class TransformList(exp: Expression) extends NamedTransformerFunction(Seq("transformListItems")) {

    import scala.collection.JavaConverters._

    override def apply(args: Array[AnyRef]): AnyRef = {
      args(0) match {
        case null => null
        case list: java.util.List[AnyRef] => list.asScala.map(a => exp.apply(Array(a))).asJava
        case list => throw new IllegalArgumentException(s"Expected a java.util.List but got $list:${list.getClass.getName}")
      }
    }

    override def withContext(ec: EvaluationContext): TransformerFunction = {
      val ewc = exp.withContext(ec)
      if (exp.eq(ewc)) { this } else { new TransformList(ewc) }
    }

    override def getInstance(args: List[Expression]): TransformerFunction = {
      args(1) match {
        case LiteralString(exp) => new TransformList(Expression(exp))
        case a => throw new IllegalArgumentException(s"${names.head} invoked with non-literal expression argument: $a")
      }
    }
  }
}
