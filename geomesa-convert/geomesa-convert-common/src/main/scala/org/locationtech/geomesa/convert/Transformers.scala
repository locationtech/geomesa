/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.convert

import java.nio.charset.StandardCharsets
import java.util.{Date, UUID}
import javax.imageio.spi.ServiceRegistry

import com.google.common.hash.Hashing
import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom._
import org.apache.commons.codec.binary.Base64
import org.geotools.geometry.jts.JTSFactoryFinder
import org.geotools.util.Converters
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter, ISODateTimeFormat}
import org.locationtech.geomesa.utils.text.{EnhancedTokenParsers, WKTUtils}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.matching.Regex

object Transformers extends EnhancedTokenParsers with Logging {

  val functionMap = mutable.HashMap[String, TransformerFn]()
  ServiceRegistry.lookupProviders(classOf[TransformerFunctionFactory]).foreach { factory =>
    factory.functions.foreach { f => functionMap.put(f.name, f) }
  }

  val EQ   = "Eq"
  val LT   = "LT"
  val GT   = "GT"
  val LTEQ = "LTEq"
  val GTEQ = "GTEq"
  val NEQ  = "NEq"

  object TransformerParser {
    private val OPEN_PAREN  = "("
    private val CLOSE_PAREN = ")"

    def string      = quotedString ^^ { s => LitString(s) } //"'" ~> "[^']*".r <~ "'".r ^^ { s => LitString(s) }
    def int         = wholeNumber ^^   { i => LitInt(i.toInt) }
    def double      = decimalNumber ^^ { d => LitDouble(d.toDouble) }
    def long        = wholeNumber ^^   { l => LitLong(l.toLong) }
    def lit         = string | int | double
    def wholeRecord = "$0" ^^ { _ => WholeRecord }
    def regexExpr   = string <~ "::r" ^^ { case LitString(s) => RegexExpr(s) }
    def column      = "$" ~> "[1-9][0-9]*".r ^^ { i => Col(i.toInt) }

    def cast2int     = expr <~ "::int"     ^^ { e => Cast2Int(e)     }
    def cast2long    = expr <~ "::long"    ^^ { e => Cast2Long(e)    }
    def cast2float   = expr <~ "::float"   ^^ { e => Cast2Float(e)   }
    def cast2double  = expr <~ "::double"  ^^ { e => Cast2Double(e)  }
    def cast2boolean = expr <~ "::boolean" ^^ { e => Cast2Boolean(e) }

    def fieldLookup = "$" ~> ident ^^ { i => FieldLookup(i) }
    def fnName      = ident ^^ { n => LitString(n) }
    def fn          = (fnName <~ OPEN_PAREN) ~ (repsep(argument, ",") <~ CLOSE_PAREN) ^^ {
      case LitString(name) ~ e =>
        FunctionExpr(functionMap(name).getInstance, e.toArray)
    }
    def strEq       = ("strEq" ~ OPEN_PAREN) ~> (transformExpr ~ "," ~ transformExpr) <~ CLOSE_PAREN ^^ {
      case l ~ "," ~ r => strBinOps(EQ)(l, r)
    }
    def numericPredicate[I](fn: String, predBuilder: (Expr, Expr) => BinaryPredicate[I])       =
      (fn ~ OPEN_PAREN) ~> (transformExpr ~ "," ~ transformExpr) <~ CLOSE_PAREN ^^ {
        case l ~ "," ~ r => predBuilder(l, r)
      }

    def getBinPreds[T](t: String, bops: Map[String, ExprToBinPred[T]]) =
      bops.map{case (n, op) => numericPredicate(t+n, op) }.reduce(_ | _)

    def binaryPred =
      strEq |
      getBinPreds("int",    intBinOps)    |
      getBinPreds("long",   longBinOps)   |
      getBinPreds("float",  floatBinOps)  |
      getBinPreds("double", doubleBinOps) |
      getBinPreds("bool",   boolBinOps)

    def andPred     = ("and" ~ OPEN_PAREN) ~> (pred ~ "," ~ pred) <~ CLOSE_PAREN ^^ {
      case l ~ "," ~ r => And(l, r)
    }
    def orPred      = ("or" ~ OPEN_PAREN) ~> (pred ~ "," ~ pred) <~ CLOSE_PAREN ^^ {
      case l ~ "," ~ r => Or(l, r)
    }
    def notPred     = ("not" ~ OPEN_PAREN) ~> pred <~ CLOSE_PAREN ^^ {
      case pred => Not(pred)
    }
    def logicPred = andPred | orPred | notPred
    def pred: Parser[Predicate] = binaryPred | logicPred
    def expr =
      fn |
        wholeRecord |
        regexExpr |
        fieldLookup |
        column |
        lit
    def transformExpr: Parser[Expr] = cast2double | cast2int | cast2boolean | cast2float | cast2long | expr
    def argument = transformExpr | string
  }

  trait Counter {
    def incSuccess(): Unit
    def getSuccess: Int

    def incFailure(): Unit
    def getFailure: Int

    def incLineCount(): Unit
    def getLineCount: Int
    def setLineCount(i: Int)
  }

  class DefaultCounter extends Counter {
    private var c: Int = 0
    private var s: Int = 0
    private var f: Int = 0
    private var skipped: Int = 0

    override def incSuccess(): Unit = s +=1
    override def getSuccess: Int = s

    override def incFailure(): Unit = f += 1
    override def getFailure: Int = f

    override def incLineCount = c += 1
    override def getLineCount: Int = c
    override def setLineCount(i: Int) = c = i
  }

  class EvaluationContext(var fieldNameMap: mutable.HashMap[String, Int], var computedFields: Array[Any], val counter: Counter = new DefaultCounter) {
    def indexOf(n: String): Int = fieldNameMap.getOrElse(n, -1)
    def lookup(i: Int) = if(i < 0) null else computedFields(i)
    def getCounter = counter
  }

  sealed trait Expr {
    def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any
  }

  sealed trait Lit[T <: Any] extends Expr {
    def value: T
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = value
  }

  case class LitString(value: String) extends Lit[String]
  case class LitInt(value: Integer) extends Lit[Integer]
  case class LitLong(value: Long) extends Lit[Long]
  case class LitFloat(value: java.lang.Float) extends Lit[java.lang.Float]
  case class LitDouble(value: java.lang.Double) extends Lit[java.lang.Double]
  case class LitBoolean(value: java.lang.Boolean) extends Lit[java.lang.Boolean]

  // TODO Better handling of casts including null handling, etc GEOMESA-982
  case class Cast2Int(e: Expr) extends Expr {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = e.eval(args).asInstanceOf[String].toInt
  }
  case class Cast2Long(e: Expr) extends Expr {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = e.eval(args).asInstanceOf[String].toLong
  }
  case class Cast2Float(e: Expr) extends Expr {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = e.eval(args).asInstanceOf[String].toFloat
  }
  case class Cast2Double(e: Expr) extends Expr {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = e.eval(args).asInstanceOf[String].toDouble
  }
  case class Cast2Boolean(e: Expr) extends Expr {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = e.eval(args).asInstanceOf[String].toBoolean
  }

  case object WholeRecord extends Expr {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = args(0)
  }

  case class Col(i: Int) extends Expr {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = args(i)
  }

  case class FieldLookup(n: String) extends Expr {
    var idx = -1
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = {
      if(idx == -1) idx = ctx.indexOf(n)
      ctx.lookup(idx)
    }
  }

  case class RegexExpr(s: String) extends Expr {
    val compiled = s.r
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = compiled
  }

  case class FunctionExpr(f: TransformerFn, arguments: Array[Expr]) extends Expr {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = f.eval(arguments.map(_.eval(args)))
  }

  sealed trait Predicate {
    def eval(args: Array[Any])(implicit ctx: EvaluationContext): Boolean
  }

  class BinaryPredicate[T](left: Expr, right: Expr, isEqual: (T, T) => Boolean) extends Predicate {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Boolean = eval(left, right, args)

    def eval(left: Expr, right: Expr, args: Array[Any])(implicit ctx: EvaluationContext): Boolean =
      isEqual(left.eval(args).asInstanceOf[T], right.eval(args).asInstanceOf[T])
  }

  def buildPred[T](f: (T, T) => Boolean): ExprToBinPred[T] = new BinaryPredicate[T](_, _, f)

  type ExprToBinPred[T] = (Expr, Expr) => BinaryPredicate[T]

  val intBinOps = Map[String, ExprToBinPred[Int]](
      EQ   -> buildPred[Int](_ == _),
      LT   -> buildPred[Int](_ < _ ),
      GT   -> buildPred[Int](_ > _ ),
      LTEQ -> buildPred[Int](_ <= _),
      GTEQ -> buildPred[Int](_ >= _),
      NEQ  -> buildPred[Int](_ != _)
    )

  val longBinOps = Map[String, ExprToBinPred[Long]](
      EQ   -> buildPred[Long](_ == _),
      LT   -> buildPred[Long](_ < _ ),
      GT   -> buildPred[Long](_ > _ ),
      LTEQ -> buildPred[Long](_ <= _),
      GTEQ -> buildPred[Long](_ >= _),
      NEQ  -> buildPred[Long](_ != _)
    )

  val floatBinOps = Map[String, ExprToBinPred[Float]](
      EQ   -> buildPred[Float](_ == _),
      LT   -> buildPred[Float](_ < _ ),
      GT   -> buildPred[Float](_ > _ ),
      LTEQ -> buildPred[Float](_ <= _),
      GTEQ -> buildPred[Float](_ >= _),
      NEQ  -> buildPred[Float](_ != _)
    )

  val doubleBinOps = Map[String, ExprToBinPred[Double]](
      EQ   -> buildPred[Double](_ == _),
      LT   -> buildPred[Double](_ < _ ),
      GT   -> buildPred[Double](_ > _ ),
      LTEQ -> buildPred[Double](_ <= _),
      GTEQ -> buildPred[Double](_ >= _),
      NEQ  -> buildPred[Double](_ != _)
    )

  val boolBinOps = Map[String, ExprToBinPred[Boolean]](
      EQ  -> buildPred[Boolean](_ == _),
      NEQ -> buildPred[Boolean](_ != _)
    )

  val strBinOps = Map[String, ExprToBinPred[String]](
      EQ  -> buildPred[String](_.equals(_)),
      NEQ -> buildPred[String]( (a, b) => !(a.equals(b)))
    )

  case class Not(p: Predicate) extends Predicate {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Boolean = !p.eval(args)
  }

  class BinaryLogicPredicate(l: Predicate, r: Predicate, f: (Boolean, Boolean) => Boolean) extends Predicate {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Boolean = f(l.eval(args), r.eval(args))
  }

  def buildBinaryLogicPredicate(f: (Boolean, Boolean) => Boolean): (Predicate, Predicate) => BinaryLogicPredicate = new BinaryLogicPredicate(_, _, f)

  val And = buildBinaryLogicPredicate(_ && _)
  val Or  = buildBinaryLogicPredicate(_ || _)

  def parseTransform(s: String): Expr = {
    logger.trace(s"Parsing transform $s")
    parse(TransformerParser.transformExpr, s).get
  }

  def parsePred(s: String): Predicate = {
    logger.trace(s"Parsing predicate $s")
    parse(TransformerParser.pred, s).get
  }

}

object TransformerFn {
  def apply(n: String)(f: Seq[Any] => Any) =
    new TransformerFn {
      override def eval(args: Array[Any])(implicit ctx: Transformers.EvaluationContext): Any = f(args)
      override def name: String = n
  }
}

trait TransformerFn {
  def name: String
  def eval(args: Array[Any])(implicit ctx: Transformers.EvaluationContext): Any
  // some transformers cache arguments that don't change, override getInstance in order
  // to return a new transformer that can cache args
  def getInstance: TransformerFn = this
}

trait TransformerFunctionFactory {
  def functions: Seq[TransformerFn]
}

class StringFunctionFactory extends TransformerFunctionFactory {

  override def functions: Seq[TransformerFn] =
    Seq(stripQuotes, strLen, trim, capitalize, lowercase, regexReplace, concat,  substr, string)

  val stripQuotes  = TransformerFn("stripQuotes")  { args => args(0).asInstanceOf[String].replaceAll("\"", "") }
  val strLen       = TransformerFn("strlen")       { args => args(0).asInstanceOf[String].length }
  val trim         = TransformerFn("trim")         { args => args(0).asInstanceOf[String].trim }
  val capitalize   = TransformerFn("capitalize")   { args => args(0).asInstanceOf[String].capitalize }
  val lowercase    = TransformerFn("lowercase")    { args => args(0).asInstanceOf[String].toLowerCase }
  val regexReplace = TransformerFn("regexReplace") { args => args(0).asInstanceOf[Regex].replaceAllIn(args(2).asInstanceOf[String], args(1).asInstanceOf[String]) }
  val concat       = TransformerFn("concat")       { args => s"${args(0)}${args(1)}" }
  val substr       = TransformerFn("substr")       { args => args(0).asInstanceOf[String].substring(args(1).asInstanceOf[Int], args(2).asInstanceOf[Int]) }
  val string       = TransformerFn("toString")     { args => args(0).toString }
}

class DateFunctionFactory extends TransformerFunctionFactory {

  override def functions: Seq[TransformerFn] =
    Seq(now, customFormatDateParser, datetime, isodate, isodatetime, basicDateTimeNoMillis, dateHourMinuteSecondMillis, millisToDate)

  val now                         = TransformerFn("now") { args => DateTime.now.toDate }
  val customFormatDateParser      = CustomFormatDateParser()
  val datetime                    = StandardDateParser("datetime", ISODateTimeFormat.dateTime().withZoneUTC())
  val isodate                     = StandardDateParser("isodate", ISODateTimeFormat.basicDate().withZoneUTC())
  val isodatetime                 = StandardDateParser("isodatetime", ISODateTimeFormat.basicDateTime().withZoneUTC())
  val basicDateTimeNoMillis       = StandardDateParser("basicDateTimeNoMillis", ISODateTimeFormat.basicDateTimeNoMillis().withZoneUTC())
  val dateHourMinuteSecondMillis  = StandardDateParser("dateHourMinuteSecondMillis", ISODateTimeFormat.dateHourMinuteSecondMillis().withZoneUTC())
  val millisToDate                = TransformerFn("millisToDate") { args => new Date(args(0).asInstanceOf[Long]) }

  case class StandardDateParser(name: String, format: DateTimeFormatter) extends TransformerFn {
    override def eval(args: Array[Any])(implicit ctx: Transformers.EvaluationContext): Any = format.parseDateTime(args(0).toString).toDate
  }

  case class CustomFormatDateParser(var format: DateTimeFormatter = null) extends TransformerFn {
    val name = "date"
    override def getInstance: CustomFormatDateParser = CustomFormatDateParser()

    override def eval(args: Array[Any])(implicit ctx: Transformers.EvaluationContext): Any = {
      if(format == null) format = DateTimeFormat.forPattern(args(0).asInstanceOf[String]).withZoneUTC()
      format.parseDateTime(args(1).asInstanceOf[String]).toDate
    }
  }
}

class GeometryFunctionFactory extends TransformerFunctionFactory {
  override def functions = Seq(pointParserFn, lineStringParserFn, polygonParserFn, geometryParserFn)

  val gf = JTSFactoryFinder.getGeometryFactory
  val pointParserFn = TransformerFn("point") { args =>
    args.length match {
      case 1 =>
        args(0) match {
          case g: Geometry => g.asInstanceOf[Point]
          case s: String   => WKTUtils.read(s).asInstanceOf[Point]
        }
      case 2 =>
        gf.createPoint(new Coordinate(args(0).asInstanceOf[Double], args(1).asInstanceOf[Double]))
      case _ =>
        throw new IllegalArgumentException(s"Invalid point conversion argument: ${args.toList}")
    }
  }

  val lineStringParserFn = TransformerFn("linestring") { args =>
    args(0) match {
      case g: Geometry => g.asInstanceOf[LineString]
      case s: String   => WKTUtils.read(s).asInstanceOf[LineString]
      case _ =>
        throw new IllegalArgumentException(s"Invalid linestring conversion argument: ${args.toList}")
    }
  }

  val polygonParserFn = TransformerFn("polygon") { args =>
    args(0) match {
      case g: Geometry => g.asInstanceOf[Polygon]
      case s: String   => WKTUtils.read(s).asInstanceOf[Polygon]
      case _ =>
        throw new IllegalArgumentException(s"Invalid polygon conversion argument: ${args.toList}")
    }
  }

  val geometryParserFn = TransformerFn("geometry") { args =>
    args(0) match {
      case s: String   => WKTUtils.read(s)
      case _ =>
        throw new IllegalArgumentException(s"Invalid geometry conversion argument: ${args.toList}")
    }
  }

}

class IdFunctionFactory extends TransformerFunctionFactory {
  override def functions = Seq(string2Bytes, MD5, uuidFn, base64)

  val string2Bytes = TransformerFn("string2bytes") { args => args(0).asInstanceOf[String].getBytes(StandardCharsets.UTF_8) }

  case object MD5 extends TransformerFn {

    override def name: String = "md5"
    val hasher = Hashing.md5()
    override def eval(args: Array[Any])(implicit ctx: Transformers.EvaluationContext): Any = hasher.hashBytes(args(0).asInstanceOf[Array[Byte]]).toString
  }

  val uuidFn = TransformerFn("uuid")   { args => UUID.randomUUID().toString }
  val base64 = TransformerFn("base64") { args => Base64.encodeBase64URLSafeString(args(0).asInstanceOf[Array[Byte]]) }

}

class LineNumberFunctionFactory extends TransformerFunctionFactory {
  override def functions = Seq(LineNumberFn())

  case class LineNumberFn() extends TransformerFn {
    override def getInstance: LineNumberFn = LineNumberFn()
    override def name: String = "lineNo"
    def eval(args: Array[Any])(implicit ctx: Transformers.EvaluationContext): Any = ctx.getCounter.getLineCount
  }

}

class MapListFunctionFactory extends TransformerFunctionFactory {
  override def functions = Seq(listFn, listParserFn, mapParserFn)

  val defaultListDelim = ","
  val defaultKVDelim   = "->"

  private def determineClazz(s: String) = s.toLowerCase match {
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

  import scala.collection.JavaConverters._

  val listFn = TransformerFn("list") { args =>
    args.toList.asJava
  }

  def convert(value: Any, clazz: Class[_]) =
    Option(Converters.convert(value, clazz))
      .getOrElse(throw new IllegalArgumentException(s"Could not convert value  '$value' to type ${clazz.getName})"))

  val listParserFn = TransformerFn("parseList") { args =>
    val clazz = determineClazz(args(0).asInstanceOf[String])
    val s = args(1).asInstanceOf[String]
    val delim = if (args.length >= 3) args(2).asInstanceOf[String] else defaultListDelim

    if (s.isEmpty) {
      List().asJava
    } else {
      s.split(delim).map(_.trim).map(convert(_, clazz)).toList.asJava
    }
  }

  val mapParserFn = TransformerFn("parseMap") { args =>
    val kv = args(0).asInstanceOf[String].split("->").map(_.trim)
    val keyClazz = determineClazz(kv(0))
    val valueClazz = determineClazz(kv(1))
    val s: String = args(1).toString
    val kvDelim: String = if (args.length >= 3) args(2).asInstanceOf[String] else defaultKVDelim
    val pairDelim: String = if (args.length >= 4) args(3).asInstanceOf[String] else defaultListDelim

    if (s.isEmpty) {
      Map().asJava
    } else {
      s.split(pairDelim)
        .map(_.split(kvDelim).map(_.trim))
        .map { case Array(key, value) =>
          (convert(key, keyClazz), convert(value, valueClazz))
        }.toMap.asJava
    }
  }

}
