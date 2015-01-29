/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.convert

import java.nio.charset.StandardCharsets
import java.util.UUID
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

  val functionMap = mutable.HashMap[String, TransformerFunctionFactory]()
  ServiceRegistry.lookupProviders(classOf[TransformerFunctionFactory]).foreach { factory =>
    factory.functions.foreach { f => functionMap.put(f, factory) }
  }

  object TransformerParser {
    private val OPEN_PAREN  = "("
    private val CLOSE_PAREN = ")"

    def string      = "'" ~> "[^']+".r <~ "'".r ^^ { s => LitString(s) }
    def int         = wholeNumber ^^ { i => LitInt(i.toInt) }
    def double      = decimalNumber ^^ { d => LitDouble(d.toDouble) }
    def lit         = string | int | double
    def wholeRecord = "$0" ^^ { _ => WholeRecord }
    def regexExpr   = string <~ "::r" ^^ { case LitString(s) => RegexExpr(s) }
    def column      = "$" ~> "[1-9][0-9]*".r ^^ { i => Col(i.toInt) }
    def cast2int    = expr <~ "::int" ^^ { e => Cast2Int(e) }
    def cast2double = expr <~ "::double" ^^ { e => Cast2Double(e) }
    def fieldLookup = "$" ~> ident ^^ { i => FieldLookup(i) }
    def fnName      = ident ^^ { n => LitString(n) }
    def fn          = (fnName <~ OPEN_PAREN) ~ (repsep(transformExpr, ",") <~ CLOSE_PAREN) ^^ {
      case LitString(name) ~ e => FunctionExpr(functionMap(name).build(name), e)
    }
    def strEq       = ("strEq" ~ OPEN_PAREN) ~> (transformExpr ~ "," ~ transformExpr) <~ CLOSE_PAREN ^^ {
      case l ~ "," ~ r => StrEQ(l, r)
    }
    def intEq       = ("intEq" ~ OPEN_PAREN) ~> (transformExpr ~ "," ~ transformExpr) <~ CLOSE_PAREN ^^ {
      case l ~ "," ~ r => IntEQ(l, r)
    }
    def intLTEq     = ("intLTEq" ~ OPEN_PAREN) ~> (transformExpr ~ "," ~ transformExpr) <~ CLOSE_PAREN ^^ {
      case l ~ "," ~ r => IntLTEQ(l, r)
    }
    def intLT       = ("intLT" ~ OPEN_PAREN) ~> (transformExpr ~ "," ~ transformExpr) <~ CLOSE_PAREN ^^ {
      case l ~ "," ~ r => IntLT(l, r)
    }
    def intGTEq     = ("intGTEq" ~ OPEN_PAREN) ~> (transformExpr ~ "," ~ transformExpr) <~ CLOSE_PAREN ^^ {
      case l ~ "," ~ r => IntGTEQ(l, r)
    }
    def intGT       = ("intGT" ~ OPEN_PAREN) ~> (transformExpr ~ "," ~ transformExpr) <~ CLOSE_PAREN ^^ {
      case l ~ "," ~ r => IntGT(l, r)
    }
    def dEq       = ("dEq" ~ OPEN_PAREN) ~> (transformExpr ~ "," ~ transformExpr) <~ CLOSE_PAREN ^^ {
      case l ~ "," ~ r => DEQ(l, r)
    }
    def dLTEq     = ("dLTEq" ~ OPEN_PAREN) ~> (transformExpr ~ "," ~ transformExpr) <~ CLOSE_PAREN ^^ {
      case l ~ "," ~ r => DLTEQ(l, r)
    }
    def dLT       = ("dLT" ~ OPEN_PAREN) ~> (transformExpr ~ "," ~ transformExpr) <~ CLOSE_PAREN ^^ {
      case l ~ "," ~ r => DLT(l, r)
    }
    def dGTEq     = ("dGTEq" ~ OPEN_PAREN) ~> (transformExpr ~ "," ~ transformExpr) <~ CLOSE_PAREN ^^ {
      case l ~ "," ~ r => DGTEQ(l, r)
    }
    def dGT       = ("dGT" ~ OPEN_PAREN) ~> (transformExpr ~ "," ~ transformExpr) <~ CLOSE_PAREN ^^ {
      case l ~ "," ~ r => DGT(l, r)
    }
    def binaryPred  = strEq | intEq | intLTEq | intLT | intGTEq | intGT | dEq | dLTEq | dLT | dGTEq | dGT
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

  class EvaluationContext(fieldNameMap: Map[String, Int], var computedFields: Array[Any]) {
    def indexOf(n: String) = fieldNameMap(n)
    def lookup(i: Int) = computedFields(i)
  }

  sealed trait Expr {
    def eval(args: Any*)(implicit ctx: EvaluationContext): Any
  }

  sealed trait Lit[T <: Any] extends Expr {
    def value: T
    override def eval(args: Any*)(implicit ctx: EvaluationContext): Any = value
  }

  case class LitString(value: String) extends Lit[String]
  case class LitInt(value: Integer) extends Lit[Integer]
  case class LitDouble(value: java.lang.Double) extends Lit[java.lang.Double]
  case class Cast2Int(e: Expr) extends Expr {
    override def eval(args: Any*)(implicit ctx: EvaluationContext): Any = e.eval(args: _*).asInstanceOf[String].toInt
  }

  case class Cast2Double(e: Expr) extends Expr {
    override def eval(args: Any*)(implicit ctx: EvaluationContext): Any = e.eval(args: _*).asInstanceOf[String].toDouble
  }

  case object WholeRecord extends Expr {
    override def eval(args: Any*)(implicit ctx: EvaluationContext): Any = args(0)
  }

  case class Col(i: Int) extends Expr {
    override def eval(args: Any*)(implicit ctx: EvaluationContext): Any = args(i)
  }

  case class FieldLookup(n: String) extends Expr {
    var idx = -1
    override def eval(args: Any*)(implicit ctx: EvaluationContext): Any = {
      if(idx == -1) idx = ctx.indexOf(n)
      ctx.lookup(idx)
    }
  }

  case class RegexExpr(s: String) extends Expr {
    val compiled = s.r
    override def eval(args: Any*)(implicit ctx: EvaluationContext): Any = compiled
  }

  case class FunctionExpr(f: TransformerFn, arguments: Seq[Expr]) extends Expr {
    override def eval(args: Any*)(implicit ctx: EvaluationContext): Any = f.eval(arguments.map(_.eval(args: _*)): _*)
  }

  sealed trait Predicate {
    def eval(args: Any*)(implicit ctx: EvaluationContext): Boolean
  }

  class BinaryPredicate[T](left: Expr, right: Expr, isEqual: (T, T) => Boolean) extends Predicate {
    override def eval(args: Any*)(implicit ctx: EvaluationContext): Boolean = eval(left, right, args: _*)

    def eval(left: Expr, right: Expr, args: Any*)(implicit ctx: EvaluationContext): Boolean =
      isEqual(left.eval(args: _*).asInstanceOf[T], right.eval(args: _*).asInstanceOf[T])
  }

  def buildPred[T](f: (T, T) => Boolean): (Expr, Expr) => BinaryPredicate[T] = new BinaryPredicate[T](_, _, f)

  val StrEQ    = buildPred[String](_.equals(_))
  val IntEQ    = buildPred[Int](_ == _)
  val IntLTEQ  = buildPred[Int](_ <= _)
  val IntLT    = buildPred[Int](_ < _)
  val IntGTEQ  = buildPred[Int](_ >= _)
  val IntGT    = buildPred[Int](_ > _)
  val DEQ      = buildPred[Double](_ == _)
  val DLTEQ    = buildPred[Double](_ <= _)
  val DLT      = buildPred[Double](_ < _)
  val DGTEQ    = buildPred[Double](_ >= _)
  val DGT      = buildPred[Double](_ > _)
  
  case class Not(p: Predicate) extends Predicate {
    override def eval(args: Any*)(implicit ctx: EvaluationContext): Boolean = !p.eval(args: _*)
  }

  class BinaryLogicPredicate(l: Predicate, r: Predicate, f: (Boolean, Boolean) => Boolean) extends Predicate {
    override def eval(args: Any*)(implicit ctx: EvaluationContext): Boolean = f(l.eval(args: _*), r.eval(args: _*))
  }

  def buildBinaryLogicPredicate(f: (Boolean, Boolean) => Boolean): (Predicate, Predicate) => BinaryLogicPredicate = new BinaryLogicPredicate(_, _, f)

  val And = buildBinaryLogicPredicate(_ && _)
  val Or  = buildBinaryLogicPredicate(_ || _)

  def parseTransform(s: String): Expr = parse(TransformerParser.transformExpr, s).get
  def parsePred(s: String): Predicate = parse(TransformerParser.pred, s).get

}

trait TransformerFn {
  def eval(args: Any*): Any
}

trait TransformerFunctionFactory {
  def functions: Seq[String]
  def build(name: String): TransformerFn
}

class StringFunctionFactory extends TransformerFunctionFactory {

  override def functions: Seq[String] =
    Seq("stripQuotes", "trim", "capitalize", "lowercase", "regexReplace", "concat", "substr", "strlen")

  def build(name: String) = name match {
    case "stripQuotes" =>
      val f: (Any*) => Any = args => args(0).asInstanceOf[String].replaceAll("\"", "")
      StringFn(f)

    case "strlen" =>
      val f: (Any*) => Any = args => args(0).asInstanceOf[String].length
      StringFn(f)

    case "trim" =>
      val f: (Any*) => Any = args => args(0).asInstanceOf[String].trim
      StringFn(f)

    case "capitalize" =>
      val f: (Any*) => Any = args => args(0).asInstanceOf[String].capitalize
      StringFn(f)

    case "lowercase" =>
      val f: (Any*) => Any = args => args(0).asInstanceOf[String].toLowerCase
      StringFn(f)

    case "regexReplace" =>
      val f: (Any*) => Any = args => args(0).asInstanceOf[Regex].replaceAllIn(args(2).asInstanceOf[String], args(1).asInstanceOf[String])
      StringFn(f)

    case "concat" =>
      val f: (Any*) => Any = args => args(0).asInstanceOf[String] + args(1).asInstanceOf[String]
      StringFn(f)

    case "substr" =>
      val f: (Any*) => Any = args => args(0).asInstanceOf[String].substring(args(1).asInstanceOf[Int], args(2).asInstanceOf[Int])
      StringFn(f)

    case e =>
      println(e)
      null
  }

  case class StringFn(f: (Any*) => Any) extends TransformerFn {
    override def eval(args: Any*): Any = f(args: _*)
  }
}

class DateFunctionFactory extends TransformerFunctionFactory {
  override def functions: Seq[String] = Seq("now", "date", "isodate", "isodatetime", "dateHourMinuteSecondMillis")

  override def build(name: String): TransformerFn = name match {
    case "now"                         => Now
    case "date"                        => new CustomFormatDateParser
    case "isodate"                     => StandardDateParser(ISODateTimeFormat.basicDate())
    case "isodatetime"                 => StandardDateParser(ISODateTimeFormat.basicDateTime())
    case "dateHourMinuteSecondMillis"  => StandardDateParser(ISODateTimeFormat.dateHourMinuteSecondMillis())
  }

  case object Now extends TransformerFn {
    override def eval(args: Any*): Any = DateTime.now().toDate
  }

  case class StandardDateParser(format: DateTimeFormatter) extends TransformerFn {
    override def eval(args: Any*): Any = format.parseDateTime(args(0).toString).toDate
  }

  class CustomFormatDateParser(var format: DateTimeFormatter = null) extends TransformerFn {
    override def eval(args: Any*): Any = {
      if(format == null) format = DateTimeFormat.forPattern(args(0).asInstanceOf[String])
      format.parseDateTime(args(1).asInstanceOf[String]).toDate
    }
  }
}

class GeometryFunctionFactory extends TransformerFunctionFactory {
  override def functions: Seq[String] = Seq("point")

  override def build(name: String): TransformerFn = name match {
    case "point" => PointParserFn
  }
  
  case object PointParserFn extends TransformerFn {
    val gf = JTSFactoryFinder.getGeometryFactory
    override def eval(args: Any*): Any = 
      gf.createPoint(new Coordinate(args(0).asInstanceOf[Double], args(1).asInstanceOf[Double]))
  }
  
}

class IdFunctionFactory extends TransformerFunctionFactory {
  override def functions: Seq[String] = Seq("md5", "uuid", "base64", "string2bytes")

  override def build(name: String): TransformerFn = name match {
    case "md5"          => MD5()
    case "uuid"         => UUIDFn()
    case "base64"       => Base64Encode()
    case "string2bytes" => String2Bytes()
  }

  case class String2Bytes() extends TransformerFn {
    override def eval(args: Any*): Any = args(0).asInstanceOf[String].getBytes(StandardCharsets.UTF_8)
  }

  case class MD5() extends TransformerFn {
    val hasher = Hashing.md5()
    override def eval(args: Any*): Any = hasher.hashBytes(args(0).asInstanceOf[Array[Byte]]).toString
  }

  case class UUIDFn() extends TransformerFn {
    override def eval(args: Any*): Any = UUID.randomUUID().toString
  }

  case class Base64Encode() extends TransformerFn {
    override def eval(args: Any*): Any = Base64.encodeBase64URLSafeString(args(0).asInstanceOf[Array[Byte]])
  }
}
