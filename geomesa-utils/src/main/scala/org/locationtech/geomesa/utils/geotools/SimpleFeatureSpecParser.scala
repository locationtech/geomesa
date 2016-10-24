/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.utils.geotools

import org.locationtech.geomesa.utils.geotools.AttributeSpec._
import org.locationtech.geomesa.utils.text.BasicParser
import org.parboiled.errors.{ErrorUtils, ParsingException}
import org.parboiled.scala.Rule1
import org.parboiled.scala.parserunners.{BasicParseRunner, ReportingParseRunner}

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
      throw new IllegalArgumentException("Spec must not be null")
    }
    val runner = if (report) { ReportingParseRunner(rule) } else { BasicParseRunner(rule) }
    val parsing = runner.run(spec.stripMargin('|').replaceAll("\\s*", ""))
    parsing.result.getOrElse {
      throw new ParsingException(s"Invalid spec string: ${ErrorUtils.printParseErrors(parsing)}")
    }
  }
}

private class SimpleFeatureSpecParser extends BasicParser {

  // Valid specs can have attributes that look like the following:
  // "id:Integer:opt1=v1:opt2=v2,*geom:Geometry:srid=4326,ct:List[String],mt:Map[String,Double]"

  import SimpleFeatureTypes.AttributeOptions.OPT_DEFAULT
  import org.parboiled.scala._

  // full simple feature spec
  def spec: Rule1[SimpleFeatureSpec] = rule {
    (oneOrMore(attribute, ",") ~ sftOptions) ~ EOI ~~> {
      (attributes, sftOpts) => SimpleFeatureSpec(attributes, sftOpts)
    }
  }

  // any attribute
  def attribute: Rule1[AttributeSpec] = rule {
    simpleAttribute | geometryAttribute | listAttribute | mapAttribute
  }

  // builds a SimpleAttributeSpec for primitive types
  private def simpleAttribute: Rule1[AttributeSpec] = rule {
    (name ~ ":" ~ simpleType ~ attributeOptions) ~~> SimpleAttributeSpec
  }

  // builds a GeometrySpec
  private def geometryAttribute: Rule1[AttributeSpec] = rule {
    (optional("*" ~ push(true)) ~ name ~ ":" ~ geometryType ~ attributeOptions) ~~> {
      (default, name, geom, opts) => {
        GeomAttributeSpec(name, geom, opts + (OPT_DEFAULT -> default.getOrElse(false).toString))
      }
    }
  }

  // builds a ListAttributeSpec for complex types
  private def listAttribute: Rule1[AttributeSpec] = rule {
    (name ~ ":" ~ listType ~ attributeOptions) ~~> ListAttributeSpec
  }

  // builds a MapAttributeSpec for complex types
  private def mapAttribute: Rule1[AttributeSpec] = rule {
    (name ~ ":" ~ mapType ~ attributeOptions) ~~> {
      (name, types, opts) => {
        MapAttributeSpec(name, types._1, types._2, opts)
      }
    }
  }

  // an attribute name
  private def name: Rule1[String] = rule { oneOrMore(noneOf(":, ")) ~> { s => s } }

  // matches any of the primitive types defined in simpleTypeMap
  // order matters so that Integer is matched before Int
  private def simpleType: Rule1[Class[_]] = rule {
    simpleTypeMap.keys.toList.sorted.reverse.map(str).reduce(_ | _) ~> simpleTypeMap.apply
  }

  // matches any of the geometry types defined in geometryTypeMap
  private def geometryType: Rule1[Class[_]] = rule {
    geometryTypeMap.keys.toList.sorted.reverse.map(str).reduce(_ | _) ~> geometryTypeMap.apply
  }

  // matches "List[Foo]" or "List" (which defaults to [String])
  private def listType: Rule1[Class[_]] = rule {
    listTypeMap.keys.map(str).reduce(_ | _) ~ optional("[" ~ simpleType ~ "]") ~~> {
      inner => inner.getOrElse(classOf[String])
    }
  }

  // matches "Map[Foo,Bar]" or Map (which defaults to [String,String])
  private def mapType: Rule1[(Class[_], Class[_])] = rule {
    mapTypeMap.keys.map(str).reduce(_ | _) ~ optional("[" ~ simpleType ~ "," ~ simpleType ~ "]") ~~> {
      types => types.getOrElse(classOf[String], classOf[String])
    }
  }

  // options appended to a specific attribute
  private def attributeOptions: Rule1[Map[String, String]] = rule {
    optional(":" ~ oneOrMore(attributeOption, ":") ~~> { _.toMap }) ~~> { _.getOrElse(Map.empty) }
  }

  // single attribute option
  private def attributeOption: Rule2[String, String] = rule {
    oneOrMore(char | anyOf(".-")) ~> { s => s } ~ "=" ~ oneOrMore(noneOf(":,;")) ~> { s => s }
  }

  // options for the simple feature type
  private def sftOptions: Rule1[Map[String, String]] = rule {
    optional(";" ~ zeroOrMore(sftOption, ",")) ~~>  { o => o.map(_.toMap).getOrElse(Map.empty) }
  }

  // single sft option
  private def sftOption: Rule1[(String, String)] = rule {
    (oneOrMore(char | ".") ~> { s => s } ~ "=" ~ sftOptionValue) ~~> { (k, v) => (k, v) }
  }

  // value for an sft option
  private def sftOptionValue: Rule1[String] = rule {
    singleQuotedString | quotedString | oneOrMore(noneOf("'\",")) ~> { s => s }
  }
}
