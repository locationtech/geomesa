/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.convert

import java.nio.charset.StandardCharsets
import java.util.{Date, UUID}
import javax.imageio.spi.ServiceRegistry

import com.google.common.hash.Hashing
import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom._
import org.apache.commons.codec.binary.Base64
import org.geotools.geometry.jts.JTSFactoryFinder
import org.geotools.util.Converters
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter, ISODateTimeFormat}
import org.locationtech.geomesa.utils.text.{EnhancedTokenParsers, WKTUtils}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.Try
import scala.util.matching.Regex

object Transformers extends EnhancedTokenParsers with LazyLogging {

  lazy val functionMap = {
    val fn = mutable.HashMap[String, TransformerFn]()
    ServiceRegistry.lookupProviders(classOf[TransformerFunctionFactory]).foreach { factory =>
      factory.functions.foreach(f => f.names.foreach(fn.put(_, f)))
    }
    fn
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

    def decimal     = """\d*\.\d+""".r
    def string      = quotedString         ^^ { s => LitString(s)            }
    def int         = wholeNumber          ^^ { i => LitInt(i.toInt)         }
    def double      = decimal <~ "[dD]?".r ^^ { d => LitDouble(d.toDouble)   }
    def long        = wholeNumber <~ "L"   ^^ { l => LitLong(l.toLong)       }
    def float       = decimal <~ "[fF]".r  ^^ { l => LitFloat(l.toFloat)     }
    def boolean     = "false|true".r       ^^ { l => LitBoolean(l.toBoolean) }
    def nulls       = "null"               ^^ { _ => LitNull                 }

    def lit         = string | float | double | long | int | boolean | nulls // order is important - most to least specific

    def wholeRecord = "$0"                   ^^ { _ => WholeRecord                  }
    def regexExpr   = string <~ "::r"        ^^ { case LitString(s) => RegexExpr(s) }
    def column      = "$" ~> "[1-9][0-9]*".r ^^ { i => Col(i.toInt)                 }

    def cast2int     = expr <~ "::int" ~ "(eger)?".r ^^ { e => Cast2Int(e)     }
    def cast2long    = expr <~ "::long"              ^^ { e => Cast2Long(e)    }
    def cast2float   = expr <~ "::float"             ^^ { e => Cast2Float(e)   }
    def cast2double  = expr <~ "::double"            ^^ { e => Cast2Double(e)  }
    def cast2boolean = expr <~ "::bool" ~ "(ean)?".r ^^ { e => Cast2Boolean(e) }

    def fieldLookup = "$" ~> ident                   ^^ { i => FieldLookup(i)  }
    def noNsfnName  = ident                          ^^ { n => LitString(n)    }
    def nsFnName    = ident ~ ":" ~ ident            ^^ { case ns ~ ":" ~ n => LitString(s"$ns:$n") }
    def fnName      = nsFnName | noNsfnName
    def tryFn       = ("try" ~ OPEN_PAREN) ~> (argument ~ "," ~ argument) <~ CLOSE_PAREN ^^ {
      case arg ~ ","  ~ fallback => TryFunctionExpr(arg, fallback)
    }
    def fn          = (fnName <~ OPEN_PAREN) ~ (repsep(argument, ",") <~ CLOSE_PAREN) ^^ {
      case LitString(name) ~ e => FunctionExpr(functionMap(name).getInstance, e.toArray)
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
      getBinPreds("int",     intBinOps)    |
      getBinPreds("integer", intBinOps)    |
      getBinPreds("long",    longBinOps)   |
      getBinPreds("float",   floatBinOps)  |
      getBinPreds("double",  doubleBinOps) |
      getBinPreds("bool",    boolBinOps)   |
      getBinPreds("boolean", boolBinOps)

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
    def expr = tryFn | fn | wholeRecord | regexExpr | fieldLookup | column | lit
    def transformExpr: Parser[Expr] = cast2double | cast2int | cast2boolean | cast2float | cast2long | expr
    def argument = transformExpr | string
  }

  trait Counter {
    def incSuccess(i: Long = 1): Unit
    def getSuccess: Long

    def incFailure(i: Long = 1): Unit
    def getFailure: Long

    // For things like Avro think of this as a recordCount as well
    def incLineCount(i: Long = 1): Unit
    def getLineCount: Long
    def setLineCount(i: Long)
  }

  class DefaultCounter extends Counter {
    private var s: Long = 0
    private var f: Long = 0
    private var c: Long = 0

    override def incSuccess(i: Long = 1): Unit = s += i
    override def getSuccess: Long = s

    override def incFailure(i: Long = 1): Unit = f += i
    override def getFailure: Long = f

    override def incLineCount(i: Long = 1) = c += i
    override def getLineCount: Long = c
    override def setLineCount(i: Long) = c = i
  }

  trait EvaluationContext {
    def get(i: Int): Any
    def set(i: Int, v: Any): Unit
    def indexOf(n: String): Int
    def counter: Counter
  }

  object EvaluationContext {
    def empty: EvaluationContext = apply(IndexedSeq.empty, Array.empty, new DefaultCounter)
    def apply(names: IndexedSeq[String], values: Array[Any], counter: Counter): EvaluationContext =
      new EvaluationContextImpl(names, values, counter)
  }

  class EvaluationContextImpl(names: IndexedSeq[String], values: Array[Any], val counter: Counter)
      extends EvaluationContext {
    def get(i: Int): Any = values(i)
    def set(i: Int, v: Any): Unit = values(i) = v
    def indexOf(n: String): Int = names.indexOf(n)
  }

  sealed trait Expr {
    def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any
    def dependenciesOf(fieldNameMap: Map[String, Field]): Seq[String]
  }

  sealed trait Lit[T <: Any] extends Expr {
    def value: T
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = value

    override def dependenciesOf(fieldNameMap: Map[String, Field]): Seq[String] = Seq()
  }

  case class LitString(value: String) extends Lit[String]
  case class LitInt(value: Integer) extends Lit[Integer]
  case class LitLong(value: Long) extends Lit[Long]
  case class LitFloat(value: java.lang.Float) extends Lit[java.lang.Float]
  case class LitDouble(value: java.lang.Double) extends Lit[java.lang.Double]
  case class LitBoolean(value: java.lang.Boolean) extends Lit[java.lang.Boolean]
  case object LitNull extends Lit[AnyRef] { override def value = null }

  sealed trait CastExpr extends Expr {
    def e: Expr
    override def dependenciesOf(fieldNameMap: Map[String, Field]): Seq[String] = e.dependenciesOf(fieldNameMap)
  }
  case class Cast2Int(e: Expr) extends CastExpr {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any =
      e.eval(args).asInstanceOf[String].toInt
  }
  case class Cast2Long(e: Expr) extends CastExpr {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any =
      e.eval(args).asInstanceOf[String].toLong
  }
  case class Cast2Float(e: Expr) extends CastExpr {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any =
      e.eval(args).asInstanceOf[String].toFloat
  }
  case class Cast2Double(e: Expr) extends CastExpr {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any =
      e.eval(args).asInstanceOf[String].toDouble
  }
  case class Cast2Boolean(e: Expr) extends CastExpr {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any =
      e.eval(args).asInstanceOf[String].toBoolean
  }

  case object WholeRecord extends Expr {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = args(0)

    override def dependenciesOf(fieldNameMap: Map[String, Field]): Seq[String] = Seq()
  }

  case class Col(i: Int) extends Expr {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = args(i)

    override def dependenciesOf(fieldNameMap: Map[String, Field]): Seq[String] = Seq()
  }

  case class FieldLookup(n: String) extends Expr {
    var doEval: EvaluationContext => Any = { ctx =>
      val idx = ctx.indexOf(n)
      doEval =
        if(idx < 0)  _  => null
        else         ec => ec.get(idx)
      doEval(ctx)
    }
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = doEval(ctx)

    override def dependenciesOf(fieldNameMap: Map[String, Field]): Seq[String] =
      Seq(n) ++ fieldNameMap.get(n).flatMap { f => Option(f.transform).map(_.dependenciesOf(fieldNameMap)) }.getOrElse(Seq.empty)
  }

  case class RegexExpr(s: String) extends Expr {
    val compiled = s.r
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = compiled

    override def dependenciesOf(fieldNameMap: Map[String, Field]): Seq[String] = Seq()
  }

  case class FunctionExpr(f: TransformerFn, arguments: Array[Expr]) extends Expr {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any =
      f.eval(arguments.map(_.eval(args)))

    override def dependenciesOf(fieldNameMap: Map[String, Field]): Seq[String] =
      arguments.flatMap(_.dependenciesOf(fieldNameMap))
  }

  case class TryFunctionExpr(toTry: Expr, fallback: Expr) extends Expr {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = {
      Try(toTry.eval(args)).getOrElse(fallback.eval(args))
    }

    override def dependenciesOf(fieldNameMap: Map[String, Field]): Seq[String] =
      toTry.dependenciesOf(fieldNameMap) ++ fallback.dependenciesOf(fieldNameMap)
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
      NEQ -> buildPred[String]((a, b) => a != b)
    )

  case class Not(p: Predicate) extends Predicate {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Boolean = !p.eval(args)
  }

  class BinaryLogicPredicate(l: Predicate, r: Predicate, f: (Boolean, Boolean) => Boolean) extends Predicate {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Boolean = f(l.eval(args), r.eval(args))
  }

  def buildBinaryLogicPredicate(f: (Boolean, Boolean) => Boolean): (Predicate, Predicate) => BinaryLogicPredicate =
    new BinaryLogicPredicate(_, _, f)

  val And = buildBinaryLogicPredicate(_ && _)
  val Or  = buildBinaryLogicPredicate(_ || _)

  def parseTransform(s: String): Expr = {
    logger.trace(s"Parsing transform $s")
    parse(TransformerParser.transformExpr, s) match {
      case Success(r, _) => r
      case Failure(e, _) => throw new IllegalArgumentException(s"Error parsing expression '$s': $e")
      case Error(e, _)   => throw new RuntimeException(s"Error parsing expression '$s': $e")
    }
  }

  def parsePred(s: String): Predicate = {
    logger.trace(s"Parsing predicate $s")
    parse(TransformerParser.pred, s) match {
      case Success(p, _) => p
      case Failure(e, _) => throw new IllegalArgumentException(s"Error parsing predicate '$s': $e")
      case Error(e, _)   => throw new RuntimeException(s"Error parsing predicate '$s': $e")
    }
  }
}

object TransformerFn {
  def apply(n: String*)(f: (Array[Any]) => Any) = new TransformerFn {
    override def eval(args: Array[Any])(implicit ctx: Transformers.EvaluationContext): Any = f(args)
    override def names: Seq[String] = n
  }
}

trait TransformerFn {
  def names: Seq[String]
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
    Seq(stripQuotes, strLen, trim, capitalize, lowercase, uppercase, regexReplace, concat, substr, string, mkstring)

  val string       = TransformerFn("toString")     { args => args(0).toString }
  val stripQuotes  = TransformerFn("stripQuotes")  { args => args(0).asInstanceOf[String].replaceAll("\"", "") }
  val trim         = TransformerFn("trim")         { args => args(0).asInstanceOf[String].trim }
  val capitalize   = TransformerFn("capitalize")   { args => args(0).asInstanceOf[String].capitalize }
  val lowercase    = TransformerFn("lowercase")    { args => args(0).asInstanceOf[String].toLowerCase }
  val uppercase    = TransformerFn("uppercase")    { args => args(0).asInstanceOf[String].toUpperCase }
  val concat       = TransformerFn("concat", "concatenate") { args => args.map(_.toString).mkString }
  val mkstring     = TransformerFn("mkstring")     { args => args.drop(1).map(_.toString).mkString(args(0).toString) }
  val regexReplace = TransformerFn("regexReplace") {
    args => args(0).asInstanceOf[Regex].replaceAllIn(args(2).asInstanceOf[String], args(1).asInstanceOf[String])
  }
  val substr = TransformerFn("substr", "substring") {
    args => args(0).asInstanceOf[String].substring(args(1).asInstanceOf[Int], args(2).asInstanceOf[Int])
  }
  val strLen = TransformerFn("strlen", "stringLength", "length") {
    args => args(0).asInstanceOf[String].length
  }
}

class DateFunctionFactory extends TransformerFunctionFactory {

  override def functions: Seq[TransformerFn] =
    Seq(now, customFormatDateParser, datetime, isodate, isodatetime, basicDateTimeNoMillis,
      dateHourMinuteSecondMillis, millisToDate, secsToDate)

  val now                        = TransformerFn("now") { args => DateTime.now.toDate }
  val millisToDate               = TransformerFn("millisToDate") { args => new Date(args(0).asInstanceOf[Long]) }
  val secsToDate                 = TransformerFn("secsToDate") { args => new Date(args(0).asInstanceOf[Long] * 1000L) }
  val customFormatDateParser     = CustomFormatDateParser()
  val datetime                   = StandardDateParser("datetime", "dateTime")(ISODateTimeFormat.dateTime().withZoneUTC())
  val isodate                    = StandardDateParser("isodate", "basicDate")(ISODateTimeFormat.basicDate().withZoneUTC())
  val isodatetime                = StandardDateParser("isodatetime", "basicDateTime")(ISODateTimeFormat.basicDateTime().withZoneUTC())
  val basicDateTimeNoMillis      = StandardDateParser("basicDateTimeNoMillis")(ISODateTimeFormat.basicDateTimeNoMillis().withZoneUTC())
  val dateHourMinuteSecondMillis = StandardDateParser("dateHourMinuteSecondMillis")(ISODateTimeFormat.dateHourMinuteSecondMillis().withZoneUTC())

  case class StandardDateParser(names: String*)(format: DateTimeFormatter) extends TransformerFn {
    override def eval(args: Array[Any])(implicit ctx: Transformers.EvaluationContext): Any =
      format.parseDateTime(args(0).toString).toDate
  }

  case class CustomFormatDateParser(var format: DateTimeFormatter = null) extends TransformerFn {
    override val names = Seq("date")
    override def getInstance: CustomFormatDateParser = CustomFormatDateParser()

    override def eval(args: Array[Any])(implicit ctx: Transformers.EvaluationContext): Any = {
      if (format == null) {
        format = DateTimeFormat.forPattern(args(0).asInstanceOf[String]).withZoneUTC()
      }
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
  override def functions = Seq(string2Bytes, md5, uuidFn, base64)

  val string2Bytes = TransformerFn("string2bytes", "stringToBytes") {
    args => args(0).asInstanceOf[String].getBytes(StandardCharsets.UTF_8)
  }
  val uuidFn = TransformerFn("uuid")   { args => UUID.randomUUID().toString }
  val base64 = TransformerFn("base64") {
    args => Base64.encodeBase64URLSafeString(args(0).asInstanceOf[Array[Byte]])
  }
  val md5 = new MD5

  class MD5 extends TransformerFn {
    override val names = Seq("md5")
    override def getInstance: MD5 = new MD5()
    val hasher = Hashing.md5()
    override def eval(args: Array[Any])(implicit ctx: Transformers.EvaluationContext): Any =
      hasher.hashBytes(args(0).asInstanceOf[Array[Byte]]).toString
  }
}

class LineNumberFunctionFactory extends TransformerFunctionFactory {
  override def functions = Seq(LineNumberFn())

  case class LineNumberFn() extends TransformerFn {
    override def getInstance: LineNumberFn = LineNumberFn()
    override val names = Seq("lineNo", "lineNumber")
    def eval(args: Array[Any])(implicit ctx: Transformers.EvaluationContext): Any = ctx.counter.getLineCount
  }
}

trait MapListParsing {
  protected def determineClazz(s: String) = s.toLowerCase match {
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

class BasicListFunction extends TransformerFunctionFactory {
  override def functions = Seq(listFn)
  import scala.collection.JavaConverters._
  val listFn = TransformerFn("list") { args => args.toList.asJava }
}

class StringMapListFunctionFactory extends TransformerFunctionFactory with MapListParsing {
  override def functions = Seq(listParserFn, mapParserFn)

  val defaultListDelim = ","
  val defaultKVDelim   = "->"

  import scala.collection.JavaConverters._

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

class CastFunctionFactory extends TransformerFunctionFactory {
  override def functions = Seq(stringToDouble, stringToInt, stringToFloat, stringToLong, stringToBoolean)

  val stringToDouble = TransformerFn("stringToDouble") {
    args => tryConvert(args(0).asInstanceOf[String], (s) => s.toDouble, args(1))
  }
  val stringToInt = TransformerFn("stringToInt", "stringToInteger") {
    args => tryConvert(args(0).asInstanceOf[String], (s) => s.toInt, args(1))
  }
  val stringToFloat = TransformerFn("stringToFloat") {
    args => tryConvert(args(0).asInstanceOf[String], (s) => s.toFloat, args(1))
  }
  val stringToLong = TransformerFn("stringToLong") {
    args => tryConvert(args(0).asInstanceOf[String], (s) => s.toLong, args(1))
  }
  val stringToBoolean = TransformerFn("stringToBool", "stringToBoolean") {
    args => tryConvert(args(0).asInstanceOf[String], (s) => s.toBoolean, args(1))
  }

  def tryConvert(s: String, conversion: (String) => Any, default: Any): Any = {
    if (s == null || s.isEmpty) {
      return default
    }
    try { conversion(s) } catch { case e: Exception => default }
  }
}
