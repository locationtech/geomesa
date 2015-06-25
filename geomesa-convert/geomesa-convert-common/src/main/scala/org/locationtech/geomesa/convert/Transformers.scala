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
import com.vividsolutions.jts.geom.Coordinate
import org.apache.commons.codec.binary.Base64
import org.geotools.geometry.jts.JTSFactoryFinder
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter, ISODateTimeFormat}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.matching.Regex
import scala.util.parsing.combinator.JavaTokenParsers

object Transformers extends JavaTokenParsers {

  val functionMap = mutable.HashMap[String, TransformerFn]()
  ServiceRegistry.lookupProviders(classOf[TransformerFunctionFactory]).foreach { factory =>
    factory.functions.foreach { f => functionMap.put(f.name, f) }
  }

  object TransformerParser {
    private val OPEN_PAREN  = "("
    private val CLOSE_PAREN = ")"

    def string      = "'" ~> "[^']*".r <~ "'".r ^^ { s => LitString(s) }
    def int         = wholeNumber ^^   { i => LitInt(i.toInt) }
    def double      = decimalNumber ^^ { d => LitDouble(d.toDouble) }
    def long        = wholeNumber ^^   { l => LitLong(l.toLong) }
    def lit         = string | int | double
    def wholeRecord = "$0" ^^ { _ => WholeRecord }
    def regexExpr   = string <~ "::r" ^^ { case LitString(s) => RegexExpr(s) }
    def column      = "$" ~> "[1-9][0-9]*".r ^^ { i => Col(i.toInt) }
    def cast2int    = expr <~ "::int" ^^ { e => Cast2Int(e) }
    def cast2double = expr <~ "::double" ^^ { e => Cast2Double(e) }
    def fieldLookup = "$" ~> ident ^^ { i => FieldLookup(i) }
    def fnName      = ident ^^ { n => LitString(n) }
    def fn          = (fnName <~ OPEN_PAREN) ~ (repsep(transformExpr, ",") <~ CLOSE_PAREN) ^^ {
      case LitString(name) ~ e => FunctionExpr(functionMap(name).getInstance, e.toArray)
    }
    def strEq       = ("strEq" ~ OPEN_PAREN) ~> (transformExpr ~ "," ~ transformExpr) <~ CLOSE_PAREN ^^ {
      case l ~ "," ~ r => StrEQ(l, r)
    }
    def numericPredicate[I](fn: String, predBuilder: (Expr, Expr) => BinaryPredicate[I])       =
      (fn ~ OPEN_PAREN) ~> (transformExpr ~ "," ~ transformExpr) <~ CLOSE_PAREN ^^ {
        case l ~ "," ~ r => predBuilder(l, r)
      }
    def intEq     = numericPredicate("intEq", IntEQ)
    def intLTEq   = numericPredicate("intLTEq", IntLTEQ)
    def intLT     = numericPredicate("intLT", IntLT)
    def intGTEq   = numericPredicate("intGTEq", IntGTEQ)
    def intGT     = numericPredicate("intGT", IntGT)
    def longEq    = numericPredicate("longEq", LonEQ)
    def longLTEq  = numericPredicate("longLTEq", LonLTEQ)
    def longLT    = numericPredicate("longLT", LonLT)
    def longGTEq  = numericPredicate("longGTEq", LonGTEQ)
    def longGT    = numericPredicate("longGT", LonGT)
    def dEq       = numericPredicate("dEq", DEQ)
    def dLTEq     = numericPredicate("dLTEq", DLTEQ)
    def dLT       = numericPredicate("dLT", DLT)
    def dGTEq     = numericPredicate("dGTEq", DGTEQ)
    def dGT       = numericPredicate("dGT", DGT)

    def binaryPred  =
      strEq |
        intEq  | intLTEq  | intLT  | intGTEq  | intGT  |
        dEq    | dLTEq    | dLT    | dGTEq    | dGT    |
        longEq | longLTEq | longLT | longGTEq | longGT

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
    def expr = fn | wholeRecord | regexExpr | fieldLookup | column | lit
    def transformExpr: Parser[Expr] = cast2double | cast2int | expr
  }

  class EvaluationContext(var fieldNameMap: mutable.HashMap[String, Int], var computedFields: Array[Any]) {
    private var count: Int = 0
    def indexOf(n: String): Int = fieldNameMap.getOrElse(n, -1)
    def lookup(i: Int) = if(i < 0) null else computedFields(i)
    def getCount(): Int = count
    def incrementCount(): Unit = count +=1
    def setCount(i: Int) = count = i
    def resetCount(): Unit = setCount(0)
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
  case class LitDouble(value: java.lang.Double) extends Lit[java.lang.Double]
  case class Cast2Int(e: Expr) extends Expr {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = e.eval(args).asInstanceOf[String].toInt
  }

  case class Cast2Double(e: Expr) extends Expr {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = e.eval(args).asInstanceOf[String].toDouble
  }
  case class Cast2Long(e: Expr) extends Expr {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = e.eval(args).asInstanceOf[String].toLong
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

  def buildPred[T](f: (T, T) => Boolean): (Expr, Expr) => BinaryPredicate[T] = new BinaryPredicate[T](_, _, f)

  val StrEQ    = buildPred[String](_.equals(_))
  val IntEQ    = buildPred[Int](_ == _)
  val IntLTEQ  = buildPred[Int](_ <= _)
  val IntLT    = buildPred[Int](_ < _)
  val IntGTEQ  = buildPred[Int](_ >= _)
  val IntGT    = buildPred[Int](_ > _)
  val LonEQ    = buildPred[Long](_ == _)
  val LonLTEQ  = buildPred[Long](_ <= _)
  val LonLT    = buildPred[Long](_ < _)
  val LonGTEQ  = buildPred[Long](_ >= _)
  val LonGT    = buildPred[Long](_ > _)
  val DEQ      = buildPred[Double](_ == _)
  val DLTEQ    = buildPred[Double](_ <= _)
  val DLT      = buildPred[Double](_ < _)
  val DGTEQ    = buildPred[Double](_ >= _)
  val DGT      = buildPred[Double](_ > _)
  
  case class Not(p: Predicate) extends Predicate {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Boolean = !p.eval(args)
  }

  class BinaryLogicPredicate(l: Predicate, r: Predicate, f: (Boolean, Boolean) => Boolean) extends Predicate {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Boolean = f(l.eval(args), r.eval(args))
  }

  def buildBinaryLogicPredicate(f: (Boolean, Boolean) => Boolean): (Predicate, Predicate) => BinaryLogicPredicate = new BinaryLogicPredicate(_, _, f)

  val And = buildBinaryLogicPredicate(_ && _)
  val Or  = buildBinaryLogicPredicate(_ || _)

  def parseTransform(s: String): Expr = parse(TransformerParser.transformExpr, s).get
  def parsePred(s: String): Predicate = parse(TransformerParser.pred, s).get

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
    Seq(stripQuotes, strLen, trim, capitalize, lowercase, regexReplace, concat,  substr)

  val stripQuotes  = TransformerFn("stripQuotes")  { args => args(0).asInstanceOf[String].replaceAll("\"", "") }
  val strLen       = TransformerFn("strlen")       { args => args(0).asInstanceOf[String].length }
  val trim         = TransformerFn("trim")         { args => args(0).asInstanceOf[String].trim }
  val capitalize   = TransformerFn("capitalize")   { args => args(0).asInstanceOf[String].capitalize }
  val lowercase    = TransformerFn("lowercase")    { args => args(0).asInstanceOf[String].toLowerCase }
  val regexReplace = TransformerFn("regexReplace") { args => args(0).asInstanceOf[Regex].replaceAllIn(args(2).asInstanceOf[String], args(1).asInstanceOf[String]) }
  val concat       = TransformerFn("concat")       { args => s"${args(0)}${args(1)}" }
  val substr       = TransformerFn("substr")       { args => args(0).asInstanceOf[String].substring(args(1).asInstanceOf[Int], args(2).asInstanceOf[Int]) }

}

class DateFunctionFactory extends TransformerFunctionFactory {

  override def functions: Seq[TransformerFn] =
    Seq(now, customFormatDateParser, datetime, isodate, isodatetime, basicDateTimeNoMillis, dateHourMinuteSecondMillis, millisToDate)

  val now = TransformerFn("now") { args => DateTime.now.toDate }
  val customFormatDateParser = CustomFormatDateParser()
  val datetime = StandardDateParser("datetime", ISODateTimeFormat.dateTime().withZoneUTC())
  val isodate = StandardDateParser("isodate", ISODateTimeFormat.basicDate().withZoneUTC())
  val isodatetime = StandardDateParser("isodatetime", ISODateTimeFormat.basicDateTime().withZoneUTC())
  val basicDateTimeNoMillis = StandardDateParser("basicDateTimeNoMillis", ISODateTimeFormat.basicDateTimeNoMillis().withZoneUTC())
  val dateHourMinuteSecondMillis = StandardDateParser("dateHourMinuteSecondMillis", ISODateTimeFormat.dateHourMinuteSecondMillis().withZoneUTC())
  val millisToDate = TransformerFn("millisToDate") { args => new Date(args(0).asInstanceOf[Long]) }

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
  override def functions = Seq(pointParserFn)

  val gf = JTSFactoryFinder.getGeometryFactory
  val pointParserFn = TransformerFn("point") { args =>
    gf.createPoint(new Coordinate(args(0).asInstanceOf[Double], args(1).asInstanceOf[Double]))
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
    def eval(args: Array[Any])(implicit ctx: Transformers.EvaluationContext): Any = ctx.getCount()
  }

}
