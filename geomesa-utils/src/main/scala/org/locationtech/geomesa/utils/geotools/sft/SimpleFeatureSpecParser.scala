/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools.sft

import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.sft.SimpleFeatureSpec._
import org.locationtech.geomesa.utils.text.BasicParser
import org.parboiled.errors.{ErrorUtils, InvalidInputError, ParsingException}
import org.parboiled.scala.parserunners.{BasicParseRunner, ReportingParseRunner}
import org.parboiled.scala.{EOI, ParsingResult, Rule1}
import org.parboiled.support.MatcherPath

import scala.annotation.tailrec

/**
  * Parser for simple feature type spec strings
  */
object SimpleFeatureSpecParser {

  private val Parser = new SimpleFeatureSpecParser()

  @throws(classOf[ParsingException])
  def parse(spec: String, report: Boolean = true): SimpleFeatureSpec = parse(spec, Parser.spec, report)

  @throws(classOf[ParsingException])
  def parseAttribute(spec: String, report: Boolean = true): AttributeSpec = parse(spec, Parser.attribute, report)

  private def parse[T](spec: String, rule: Rule1[T], report: Boolean): T = {
    if (spec == null) {
      throw new IllegalArgumentException("Invalid spec string: null")
    }
    val runner = if (report) { ReportingParseRunner(rule) } else { BasicParseRunner(rule) }
    val parsing = runner.run(spec.stripMargin('|').replaceAll("\\s*", ""))
    parsing.result.getOrElse(throw new ParsingException(constructErrorMessage(parsing, report)))
  }

  private def constructErrorMessage[T](result: ParsingResult[T], report: Boolean): String = {
    lazy val fallback = s"Invalid spec string: ${ErrorUtils.printParseErrors(result)}"
    if (!report) { fallback } else {
      result.parseErrors.collectFirst { case e: InvalidInputError =>
        import scala.collection.JavaConversions._
        // determine what paths the parser partially matched
        val matchers = e.getFailedMatchers.map(getFailedMatcher).distinct
        if (matchers.isEmpty) {
          s"Invalid spec string at index ${e.getStartIndex}."
        } else {
          val expected = if (matchers.lengthCompare(1) > 0) { s"one of: ${matchers.mkString(", ")}" } else { matchers.head }
          s"Invalid spec string at index ${e.getStartIndex}. Expected $expected."
        }
      }.getOrElse(fallback)
    }
  }

  /**
    * Start at the failure point and go up the parse tree until
    * we get to a meaningful parser, as defined in ReportableParserMessages.
    * We should always match at least the top-level parser (spec or attribute)
    *
    * @param path failed matcher path
    * @return message indicating the last-matched matcher
    */
  @tailrec
  private def getFailedMatcher(path: MatcherPath): String = {
    ReportableParserMessages.get(path.element.matcher.getLabel) match {
      case Some(m) => m
      case None    => getFailedMatcher(path.parent)
    }
  }

  // error messages corresponding to our rule labels
  private val ReportableParserMessages = Map(
    Parser.spec            -> "specification",
    Parser.attribute       -> "attribute",
    Parser.name            -> "attribute name",
    Parser.mapType         -> "attribute type binding",
    Parser.listType        -> "attribute type binding",
    Parser.simpleType      -> "attribute type binding",
    Parser.geometryType    -> "geometry type binding",
    Parser.sftOption       -> "feature type option",
    Parser.attributeOption -> "attribute option",
    Parser.default         -> "'*'",
    EOI                    -> "end of spec"
  ).map { case (k, v) => k.matcher.getLabel -> v }
}

private class SimpleFeatureSpecParser extends BasicParser {

  // Valid specs can have attributes that look like the following:
  // "id:Integer:opt1=v1:opt2=v2,*geom:Geometry:srid=4326,ct:List[String],mt:Map[String,Double]"

  import SimpleFeatureTypes.AttributeOptions.OPT_DEFAULT
  import org.parboiled.scala._

  // full simple feature spec
  def spec: Rule1[SimpleFeatureSpec] = rule("Specification") {
    (zeroOrMore(attribute, ",") ~ sftOptions) ~ EOI ~~> {
      (attributes, sftOpts) => SimpleFeatureSpec(attributes, sftOpts)
    }
  }

  // any attribute
  def attribute: Rule1[AttributeSpec] = rule("Attribute") {
    simpleAttribute | geometryAttribute | listAttribute | mapAttribute
  }

  // builds a SimpleAttributeSpec for primitive types
  private def simpleAttribute: Rule1[AttributeSpec] = rule("SimpleAttribute") {
    (name ~ simpleType ~ attributeOptions) ~~> SimpleAttributeSpec
  }

  // builds a GeometrySpec
  private def geometryAttribute: Rule1[AttributeSpec] = rule("GeometryAttribute") {
    (default ~ name ~ geometryType ~ attributeOptions) ~~> {
      (default, name, geom, opts) => {
        GeomAttributeSpec(name, geom, opts + (OPT_DEFAULT -> default.getOrElse(false).toString))
      }
    }
  }

  // builds a ListAttributeSpec for complex types
  private def listAttribute: Rule1[AttributeSpec] = rule("ListAttribute") {
    (name ~ listType ~ attributeOptions) ~~> ListAttributeSpec
  }

  // builds a MapAttributeSpec for complex types
  private def mapAttribute: Rule1[AttributeSpec] = rule("MapAttribute") {
    (name ~ mapType ~ attributeOptions) ~~> {
      (name, types, opts) => {
        MapAttributeSpec(name, types._1, types._2, opts)
      }
    }
  }

  // an attribute name
  private def name: Rule1[String] = rule("AttributeName") { oneOrMore(noneOf("*:, ")) ~> { s => s } }

  private def default: Rule1[Option[Boolean]] = rule("DefaultAttribute") { optional("*" ~ push(true)) }

  private def simpleType: Rule1[Class[_]] = rule("SimpleTypeBinding") { ":" ~ simpleBinding }

  // matches any of the primitive types defined in simpleTypeMap
  // order matters so that Integer is matched before Int
  private def simpleBinding: Rule1[Class[_]] = rule("SimpleBinding") {
    simpleTypeMap.keys.toList.sorted.reverse.map(str).reduce(_ | _) ~> simpleTypeMap.apply
  }

  // matches any of the geometry types defined in geometryTypeMap
  private def geometryType: Rule1[Class[_]] = rule("GeometryTypeBinding") {
    ":" ~ geometryTypeMap.keys.toList.sorted.reverse.map(str).reduce(_ | _) ~> geometryTypeMap.apply
  }

  // matches "List[Foo]" or "List" (which defaults to [String])
  private def listType: Rule1[Class[_]] = rule("ListTypeBinding") {
    ":" ~ listTypeMap.keys.map(str).reduce(_ | _) ~ optional("[" ~ simpleBinding ~ "]") ~~> {
      inner => inner.getOrElse(classOf[String])
    }
  }

  // matches "Map[Foo,Bar]" or Map (which defaults to [String,String])
  private def mapType: Rule1[(Class[_], Class[_])] = rule("MapTypeBinding") {
    ":" ~ mapTypeMap.keys.map(str).reduce(_ | _) ~ optional("[" ~ simpleBinding ~ "," ~ simpleBinding ~ "]") ~~> {
      types => types.getOrElse(classOf[String], classOf[String])
    }
  }

  // options appended to a specific attribute
  private def attributeOptions: Rule1[Map[String, String]] = rule("AttributeOptions") {
    optional(":" ~ oneOrMore(attributeOption, ":") ~~> { _.toMap }) ~~> { _.getOrElse(Map.empty) }
  }

  // single attribute option
  private def attributeOption: Rule2[String, String] = rule("AttributeOption") {
    oneOrMore(char | anyOf(".-")) ~> { s => s } ~ "=" ~ (quotedString | singleQuotedString | unquotedString)
  }

  // options for the simple feature type
  private def sftOptions: Rule1[Map[String, String]] = rule("FeatureTypeOptions") {
    optional(";" ~ zeroOrMore(sftOption, ",")) ~~>  { o => o.map(_.toMap).getOrElse(Map.empty) }
  }

  // single sft option
  private def sftOption: Rule1[(String, String)] = rule("FeatureTypeOption") {
    (oneOrMore(char | anyOf(".-")) ~> { s => s } ~ "=" ~ sftOptionValue) ~~> { (k, v) => (k, v) }
  }

  // value for an sft option
  private def sftOptionValue: Rule1[String] = rule("FeatureTypeOptionValue") {
    singleQuotedString | quotedString | oneOrMore(noneOf("'\",")) ~> { s => s }
  }
}
