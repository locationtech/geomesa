/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert

import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentHashMap
import java.util.{Date, DoubleSummaryStatistics, Locale, ServiceLoader, UUID}

import com.google.common.hash.Hashing
import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom._
import org.apache.commons.codec.binary.Base64
import org.geotools.geometry.jts.{JTS, JTSFactoryFinder}
import org.geotools.referencing.CRS
import org.geotools.util.Converters
import org.locationtech.geomesa.utils.text.{DateParsing, EnhancedTokenParsers, WKTUtils}
import org.opengis.referencing.operation.MathTransform

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.Try
import scala.util.matching.Regex

object Transformers extends EnhancedTokenParsers with LazyLogging {

  lazy val functionMap = {
    val fn = mutable.HashMap[String, TransformerFn]()
    ServiceLoader.load(classOf[TransformerFunctionFactory]).foreach { factory =>
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

    def decimal     = """-?\d*\.\d+""".r
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
    def cast2string  = expr <~ "::string"            ^^ { e => Cast2String(e)  }

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
      pred => Not(pred)
    }
    def logicPred = andPred | orPred | notPred
    def pred: Parser[Predicate] = binaryPred | logicPred
    def expr = tryFn | fn | wholeRecord | regexExpr | fieldLookup | column | lit
    def transformExpr: Parser[Expr] = cast2double | cast2int | cast2boolean | cast2float | cast2long | cast2string | expr
    def argument = transformExpr | string
  }

  sealed trait Expr {
    def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any

    /**
      * Gets the field dependencies that this expr relies on
      *
      * @param stack current field stack, used to detect circular dependencies
      * @param fieldNameMap fields lookup
      * @return dependencies
      */
    def dependenciesOf(stack: Set[Field], fieldNameMap: Map[String, Field]): Set[Field]
  }

  sealed trait Lit[T <: Any] extends Expr {
    def value: T
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = value
    override def dependenciesOf(stack: Set[Field], fieldMap: Map[String, Field]): Set[Field] = Set.empty
    override def toString: String = String.valueOf(value)
  }

  case class LitString(value: String) extends Lit[String] {
    override def toString: String = s"'${String.valueOf(value)}'"
  }
  case class LitInt(value: Integer) extends Lit[Integer]
  case class LitLong(value: Long) extends Lit[Long]
  case class LitFloat(value: java.lang.Float) extends Lit[java.lang.Float]
  case class LitDouble(value: java.lang.Double) extends Lit[java.lang.Double]
  case class LitBoolean(value: java.lang.Boolean) extends Lit[java.lang.Boolean]
  case object LitNull extends Lit[AnyRef] { override def value = null }

  sealed trait CastExpr extends Expr {
    def e: Expr
    override def dependenciesOf(stack: Set[Field], fieldMap: Map[String, Field]): Set[Field] =
      e.dependenciesOf(stack, fieldMap)
  }
  case class Cast2Int(e: Expr) extends CastExpr {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Int =
      e.eval(args) match {
        case int: Int       => int
        case double: Double => double.toInt
        case float: Float   => float.toInt
        case long: Long     => long.toInt
        case any: Any       => any.toString.toInt
      }
    override def toString: String = s"$e::int"
  }
  case class Cast2Long(e: Expr) extends CastExpr {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Long =
      e.eval(args) match {
        case int: Int       => int.toLong
        case double: Double => double.toLong
        case float: Float   => float.toLong
        case long: Long     => long
        case any: Any       => any.toString.toLong
      }
    override def toString: String = s"$e::long"
  }
  case class Cast2Float(e: Expr) extends CastExpr {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Float =
      e.eval(args) match {
        case int: Int       => int.toFloat
        case double: Double => double.toFloat
        case float: Float   => float
        case long: Long     => long.toFloat
        case any: Any       => any.toString.toFloat
      }
    override def toString: String = s"$e::float"
  }
  case class Cast2Double(e: Expr) extends CastExpr {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Double =
      e.eval(args) match {
        case int: Int       => int.toDouble
        case double: Double => double
        case float: Float   => float.toDouble
        case long: Long     => long.toDouble
        case any: Any       => any.toString.toDouble
      }
    override def toString: String = s"$e::double"
  }
  case class Cast2Boolean(e: Expr) extends CastExpr {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any =
      e.eval(args).asInstanceOf[String].toBoolean
    override def toString: String = s"$e::boolean"
  }
  case class Cast2String(e: Expr) extends CastExpr {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any =
      e.eval(args).toString
    override def toString: String = s"$e::string"
  }

  case object WholeRecord extends Expr {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = args(0)
    override def dependenciesOf(stack: Set[Field], fieldMap: Map[String, Field]): Set[Field] = Set.empty
  }

  case class Col(i: Int) extends Expr {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = args(i)
    override def dependenciesOf(stack: Set[Field], fieldMap: Map[String, Field]): Set[Field] = Set.empty
    override def toString: String = s"$$$i"
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

    override def dependenciesOf(stack: Set[Field], fieldMap: Map[String, Field]): Set[Field] = {
      fieldMap.get(n) match {
        case None => Set.empty
        case Some(field) =>
          if (stack.contains(field)) {
            throw new IllegalArgumentException(s"Cyclical dependency detected in field $field")
          } else {
            Option(field.transform).toSeq.flatMap(_.dependenciesOf(stack + field, fieldMap)).toSet + field
          }
      }
    }

    override def toString: String = s"$$$n"
  }

  case class RegexExpr(s: String) extends Expr {
    val compiled = s.r
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = compiled
    override def dependenciesOf(stack: Set[Field], fieldMap: Map[String, Field]): Set[Field] = Set.empty
    override def toString: String = s"$s::r"
  }

  case class FunctionExpr(f: TransformerFn, arguments: Array[Expr]) extends Expr {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any =
      f.eval(arguments.map(_.eval(args)))
    override def dependenciesOf(stack: Set[Field], fieldMap: Map[String, Field]): Set[Field] =
      arguments.flatMap(_.dependenciesOf(stack, fieldMap)).toSet
    override def toString: String = s"${f.names.head}${arguments.mkString("(", ",", ")")}"
  }

  case class TryFunctionExpr(toTry: Expr, fallback: Expr) extends Expr {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = {
      Try(toTry.eval(args)).getOrElse(fallback.eval(args))
    }
    override def dependenciesOf(stack: Set[Field], fieldMap: Map[String, Field]): Set[Field] =
      toTry.dependenciesOf(stack, fieldMap) ++ fallback.dependenciesOf(stack, fieldMap)
    override def toString: String = s"try($toTry,$fallback)"
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
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = f(args)
    override def names: Seq[String] = n
  }
}

trait TransformerFn {
  def names: Seq[String]
  def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any
  // some transformers cache arguments that don't change, override getInstance in order
  // to return a new transformer that can cache args
  def getInstance: TransformerFn = this
}

trait TransformerFunctionFactory {
  def functions: Seq[TransformerFn]
}

class StringFunctionFactory extends TransformerFunctionFactory {

  override def functions: Seq[TransformerFn] =
    Seq(stripQuotes, strLen, trim, capitalize, lowercase, uppercase, regexReplace, concat, substr, string, mkstring, emptyToNull)

  val string       = TransformerFn("toString")     { args => args(0).toString }
  val stripQuotes  = TransformerFn("stripQuotes")  { args => args(0).asInstanceOf[String].replaceAll("\"", "") }
  val trim         = TransformerFn("trim")         { args => args(0).asInstanceOf[String].trim }
  val capitalize   = TransformerFn("capitalize")   { args => args(0).asInstanceOf[String].capitalize }
  val lowercase    = TransformerFn("lowercase")    { args => args(0).asInstanceOf[String].toLowerCase }
  val uppercase    = TransformerFn("uppercase")    { args => args(0).asInstanceOf[String].toUpperCase }
  val concat       = TransformerFn("concat", "concatenate") { args => args.map(_.toString).mkString }
  val mkstring     = TransformerFn("mkstring")     { args => args.drop(1).map(_.toString).mkString(args(0).toString) }
  val emptyToNull  = TransformerFn("emptyToNull")  { args => Option(args(0)).map(_.toString).filterNot(_.trim.isEmpty).orNull }
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

  import java.time.{ZoneOffset, ZonedDateTime}
  import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
  import java.time.temporal.ChronoField

  // yyyy-MM-dd'T'HH:mm:ss.SSSZZ (ZZ is time zone with colon)
  private val dateTimeFormat =
    new DateTimeFormatterBuilder()
      .parseCaseInsensitive()
      .append(DateTimeFormatter.ISO_LOCAL_DATE)
      .parseLenient()
      .appendLiteral('T')
      .appendValue(ChronoField.HOUR_OF_DAY, 2)
      .appendLiteral(':')
      .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
      .appendLiteral(':')
      .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
      .appendFraction(ChronoField.MILLI_OF_SECOND, 3, 3, true)
      .optionalStart()
      .appendOffsetId()
      .toFormatter(Locale.US)
      .withZone(ZoneOffset.UTC)

  // yyyyMMdd
  private val basicDateFormat = DateTimeFormatter.BASIC_ISO_DATE.withZone(ZoneOffset.UTC)

  // yyyyMMdd'T'HHmmss.SSSZ
  private val basicDateTimeFormat =
    new DateTimeFormatterBuilder()
      .parseCaseInsensitive()
      .appendValue(ChronoField.YEAR, 4)
      .appendValue(ChronoField.MONTH_OF_YEAR, 2)
      .appendValue(ChronoField.DAY_OF_MONTH, 2)
      .appendLiteral('T')
      .appendValue(ChronoField.HOUR_OF_DAY, 2)
      .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
      .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
      .appendFraction(ChronoField.MILLI_OF_SECOND, 3, 3, true)
      .optionalStart()
      .appendOffsetId()
      .toFormatter(Locale.US)
      .withZone(ZoneOffset.UTC)

  // yyyyMMdd'T'HHmmssZ
  private val basicDateTimeNoMillisFormat =
    new DateTimeFormatterBuilder()
      .parseCaseInsensitive()
      .appendValue(ChronoField.YEAR, 4)
      .appendValue(ChronoField.MONTH_OF_YEAR, 2)
      .appendValue(ChronoField.DAY_OF_MONTH, 2)
      .appendLiteral('T')
      .appendValue(ChronoField.HOUR_OF_DAY, 2)
      .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
      .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
      .optionalStart()
      .appendOffsetId()
      .toFormatter(Locale.US)
      .withZone(ZoneOffset.UTC)

  // yyyy-MM-dd'T'HH:mm:ss.SSS
  private val dateHourMinuteSecondMillisFormat =
    new DateTimeFormatterBuilder()
        .parseCaseInsensitive()
        .append(DateTimeFormatter.ISO_LOCAL_DATE)
        .parseLenient()
        .appendLiteral('T')
        .appendValue(ChronoField.HOUR_OF_DAY, 2)
        .appendLiteral(':')
        .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
        .appendLiteral(':')
        .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
        .appendFraction(ChronoField.MILLI_OF_SECOND, 3, 3, true)
        .toFormatter(Locale.US)
        .withZone(ZoneOffset.UTC)

  override def functions: Seq[TransformerFn] =
    Seq(now, customFormatDateParser, datetime, isodate, isodatetime, basicDateTimeNoMillis,
      dateHourMinuteSecondMillis, millisToDate, secsToDate, dateToString)

  private val now                        = TransformerFn("now") { _ => Date.from(ZonedDateTime.now(ZoneOffset.UTC).toInstant) }
  private val millisToDate               = TransformerFn("millisToDate") { args => new Date(args(0).asInstanceOf[Long]) }
  private val secsToDate                 = TransformerFn("secsToDate") { args => new Date(args(0).asInstanceOf[Long] * 1000L) }
  private val customFormatDateParser     = CustomFormatDateParser()
  private val datetime                   = StandardDateParser("datetime", "dateTime")(dateTimeFormat)
  private val isodate                    = StandardDateParser("isodate", "basicDate")(basicDateFormat)
  private val isodatetime                = StandardDateParser("isodatetime", "basicDateTime")(basicDateTimeFormat)
  private val basicDateTimeNoMillis      = StandardDateParser("basicDateTimeNoMillis")(basicDateTimeNoMillisFormat)
  private val dateHourMinuteSecondMillis = StandardDateParser("dateHourMinuteSecondMillis")(dateHourMinuteSecondMillisFormat)

  private val dateToString = DateToString()

  case class StandardDateParser(names: String*)(format: DateTimeFormatter) extends TransformerFn {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any =
      DateParsing.parseDate(args(0).toString, format)
  }

  case class CustomFormatDateParser(var format: DateTimeFormatter = null) extends TransformerFn {
    override val names = Seq("date")
    override def getInstance: CustomFormatDateParser = CustomFormatDateParser()

    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = {
      if (format == null) {
        format = DateTimeFormatter.ofPattern(args(0).asInstanceOf[String]).withZone(ZoneOffset.UTC)
      }
      DateParsing.parseDate(args(1).toString, format)
    }
  }

  case class DateToString(var format: DateTimeFormatter = null) extends TransformerFn {
    override val names = Seq("dateToString")
    override def getInstance: DateToString = DateToString()

    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = {
      if (format == null) {
        format = DateTimeFormatter.ofPattern(args(0).asInstanceOf[String]).withZone(ZoneOffset.UTC)
      }
      DateParsing.formatDate(args(1).asInstanceOf[java.util.Date], format)
    }
  }
}

class GeometryFunctionFactory extends TransformerFunctionFactory {
  override def functions = Seq(pointParserFn,
    multiPointParserFn,
    lineStringParserFn,
    multiLineStringParserFn,
    polygonParserFn,
    multiPolygonParserFn,
    geometryParserFn,
    geometryCollectionParserFn,
    projectFromParserFn)

  private val gf = JTSFactoryFinder.getGeometryFactory

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

  val multiPointParserFn = TransformerFn("multipoint") { args =>
    args(0) match {
      case g: Geometry => g.asInstanceOf[MultiPoint]
      case s: String   => WKTUtils.read(s).asInstanceOf[MultiPoint]
      case _ =>
        throw new IllegalArgumentException(s"Invalid multipoint conversion argument: ${args.toList}")
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

  val multiLineStringParserFn = TransformerFn("multilinestring") { args =>
    args(0) match {
      case g: Geometry => g.asInstanceOf[MultiLineString]
      case s: String   => WKTUtils.read(s).asInstanceOf[MultiLineString]
      case _ =>
        throw new IllegalArgumentException(s"Invalid multilinestring conversion argument: ${args.toList}")
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

  val multiPolygonParserFn = TransformerFn("multipolygon") { args =>
    args(0) match {
      case g: Geometry => g.asInstanceOf[MultiPolygon]
      case s: String   => WKTUtils.read(s).asInstanceOf[MultiPolygon]
      case _ =>
        throw new IllegalArgumentException(s"Invalid multipolygon conversion argument: ${args.toList}")
    }
  }

  val geometryParserFn = TransformerFn("geometry") { args =>
    args(0) match {
      case g: Geometry => g.asInstanceOf[Geometry]
      case s: String   => WKTUtils.read(s)
      case _ =>
        throw new IllegalArgumentException(s"Invalid geometry conversion argument: ${args.toList}")
    }
  }

  val geometryCollectionParserFn = TransformerFn("geometrycollection") { args =>
    args(0) match {
      case g: Geometry => g.asInstanceOf[GeometryCollection]
      case s: String   => WKTUtils.read(s)
      case _ =>
        throw new IllegalArgumentException(s"Invalid geometrycollection conversion argument: ${args.toList}")
    }
  }

  val projectFromParserFn = new TransformerFn {

    private val cache = new ConcurrentHashMap[String, MathTransform]

    override val names: Seq[String] = Seq("projectFrom")

    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = {
      import org.locationtech.geomesa.utils.geotools.CRS_EPSG_4326

      val epsg = args(0).asInstanceOf[String]
      val geom = args(1).asInstanceOf[Geometry]
      val lenient = if (args.length > 2) { java.lang.Boolean.parseBoolean(args(2).toString) } else { true }
      // transforms should be thread safe according to https://sourceforge.net/p/geotools/mailman/message/32123017/
      val transform = cache.getOrElseUpdate(s"$epsg:$lenient",
        CRS.findMathTransform(CRS.decode(epsg), CRS_EPSG_4326, lenient))
      JTS.transform(geom, transform)
    }
  }
}

class IdFunctionFactory extends TransformerFunctionFactory {
  override def functions = Seq(string2Bytes, md5, uuidFn, base64, murmur3_32, murmur3_64)

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
    private val hasher = Hashing.md5()
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any =
      hasher.hashBytes(args(0).asInstanceOf[Array[Byte]]).toString
  }

  class Murmur3_32 extends TransformerFn {
    private val hasher = Hashing.murmur3_32()
    override val names: Seq[String] = Seq("murmur3_32")
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = {
      hasher.hashString(args(0).toString, StandardCharsets.UTF_8)
    }
  }
  val murmur3_32 = new Murmur3_32

  class Murmur3_64 extends TransformerFn {
    private val hasher = Hashing.murmur3_128()
    override val names: Seq[String] = Seq("murmur3_64")
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = {
      hasher.hashString(args(0).toString, StandardCharsets.UTF_8).asLong()
    }
  }
  val murmur3_64 = new Murmur3_64
}

class LineNumberFunctionFactory extends TransformerFunctionFactory {
  override def functions = Seq(LineNumberFn())

  case class LineNumberFn() extends TransformerFn {
    override def getInstance: LineNumberFn = LineNumberFn()
    override val names = Seq("lineNo", "lineNumber")
    def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = ctx.counter.getLineCount
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

class CollectionFunctionFactory extends TransformerFunctionFactory {
  import scala.collection.JavaConverters._

  override def functions = Seq(listFn, mapValueFunction)

  val listFn = TransformerFn("list") { args => args.toList.asJava }
  val mapValueFunction = TransformerFn("mapValue") {
    args => args(0).asInstanceOf[java.util.Map[Any, Any]].get(args(1))
  }
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

class MathFunctionFactory extends TransformerFunctionFactory {
  override def functions = Seq(add, subtract, multiply, divide, mean, min, max)

  def parseDouble(v: Any): Double = {
    v match {
      case int: Int       => int.toDouble
      case double: Double => double
      case float: Float   => float.toDouble
      case long: Long     => long.toDouble
      case string: String => string.toDouble
      case any: Any       => any.toString.toDouble
    }
  }

  val add = TransformerFn("add") { args =>
    var s: Double = 0.0
    args.foreach(s += parseDouble(_))
    s
  }

  val multiply = TransformerFn("multiply") { args =>
    var s: Double = 1.0
    args.foreach(s *= parseDouble(_))
    s
  }

  val subtract = TransformerFn("subtract") { args =>
    var s: Double = parseDouble(args(0))
    args.drop(1).foreach(s -= parseDouble(_))
    s
  }

  val divide = TransformerFn("divide") { args =>
    var s: Double = parseDouble(args(0))
    args.drop(1).foreach(s /= parseDouble(_))
    s
  }

  val mean = TransformerFn("mean") { args =>
    val stats = new DoubleSummaryStatistics
    args.map(parseDouble).foreach { d => stats.accept(d) }
    stats.getAverage
  }

  val min = TransformerFn("min") { args =>
    val stats = new DoubleSummaryStatistics
    args.map(parseDouble).foreach { d => stats.accept(d) }
    stats.getMin
  }

  val max = TransformerFn("max") { args =>
    val stats = new DoubleSummaryStatistics
    args.map(parseDouble).foreach { d => stats.accept(d) }
    stats.getMax
  }
}

class EnrichmentCacheFunctionFactory extends TransformerFunctionFactory {
  override def functions = Seq(cacheLookup)

  val cacheLookup = new TransformerFn {
    override def names: Seq[String] = Seq("cacheLookup")
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = {
      val cache = ctx.getCache(args(0).asInstanceOf[String])
      cache.get(Array(args(1).asInstanceOf[String], args(2).asInstanceOf[String]))
    }
  }

}
