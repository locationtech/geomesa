/***********************************************************************
 * Copyright (c) 2013-2025 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.kryo.json

import org.junit.runner.RunWith
import org.locationtech.geomesa.features.kryo.json.JsonPathParser._
import org.parboiled.errors.ParsingException
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.util.regex.Pattern

@RunWith(classOf[JUnitRunner])
class JsonPathParserTest extends Specification {

  import PathFilter._

  "JsonPathParser" should {
    "not parse invalid paths" in {
      JsonPathParser.parse("$.$") must throwA[ParsingException]
      JsonPathParser.parse("$.foo foo") must throwA[ParsingException]
    }
    "correctly parse and print attribute paths" in {
      val raw = "$.foo"
      val path = JsonPathParser.parse(raw)
      path mustEqual JsonPath(Seq(PathAttribute("foo")))
      JsonPathParser.print(path) mustEqual raw
    }
    "correctly parse and print attribute bracket paths" in {
      val tests = Seq(
        "$[foo]"         -> JsonPath(Seq(PathAttribute("foo", bracketed = true))),
        "$[foo_bar]"     -> JsonPath(Seq(PathAttribute("foo_bar", bracketed = true))),
        "$['foo']"       -> JsonPath(Seq(PathAttribute("foo", bracketed = true))),
        "$['foo_bar']"   -> JsonPath(Seq(PathAttribute("foo_bar", bracketed = true))),
        "$['foo-bar 0']" -> JsonPath(Seq(PathAttribute("foo-bar 0", bracketed = true))),
      )
      foreach(tests) { case (raw, expected) =>
        JsonPathParser.parse(raw) mustEqual expected
        if (raw.contains('\'')) {
          JsonPathParser.print(expected) mustEqual raw
        } else {
          // unquoted attributes isn't valid syntax, but we still accept it and then fix quotes in the output
          JsonPathParser.print(expected) mustEqual raw.replaceAll("\\[", "['").replaceAll("]", "']")
        }
      }
    }
    "correctly parse and print array index paths" in {
      val raw = "$.foo[2]"
      val path = JsonPathParser.parse(raw)
      path mustEqual JsonPath(Seq(PathAttribute("foo"), PathIndices(Seq(2))))
      JsonPathParser.print(path) mustEqual raw
    }
    "correctly parse and print multiple array index paths" in {
      val raw = "$.foo[2,3,4]"
      val path = JsonPathParser.parse(raw)
      path mustEqual JsonPath(Seq(PathAttribute("foo"), PathIndices(Seq(2, 3, 4))))
      JsonPathParser.print(path) mustEqual raw
    }
    "correctly parse and print array index range paths" in {
      val raw = "$.foo[2:4]"
      val path = JsonPathParser.parse(raw)
      path mustEqual JsonPath(Seq(PathAttribute("foo"), PathIndexRange(Some(2), Some(4))))
      JsonPathParser.print(path) mustEqual raw
    }
    "correctly parse and print wildcards paths" in {
      val raw = "$.store.*" // all things, both books and bicycles
      val path = JsonPathParser.parse(raw)
      path mustEqual JsonPath(Seq(PathAttribute("store"), PathAttributeWildCard))
      JsonPathParser.print(path) mustEqual raw
    }
    "correctly parse and print wildcards in attribute paths" in {
      val raw = "$.foo.*.name"
      val path = JsonPathParser.parse(raw)
      path mustEqual JsonPath(Seq(PathAttribute("foo"), PathAttributeWildCard, PathAttribute("name")))
      JsonPathParser.print(path) mustEqual raw
    }
    "correctly parse and print wildcards in array index paths" in {
      val raw = "$.store.book[*].author" // the authors of all books
      val path = JsonPathParser.parse(raw)
      path mustEqual JsonPath(Seq(PathAttribute("store"), PathAttribute("book"), PathIndexWildCard, PathAttribute("author")))
      JsonPathParser.print(path) mustEqual raw
    }
    "correctly parse and print deep scan attributes" in {
      val raw = "$..author" // all authors
      val path = JsonPathParser.parse(raw)
      path mustEqual JsonPath(Seq(PathDeepScan, PathAttribute("author")))
      JsonPathParser.print(path) mustEqual raw
    }
    "correctly parse and print deep scan attributes with indices" in {
      val raw = "$..foo[0]"
      val path = JsonPathParser.parse(raw)
      path mustEqual JsonPath(Seq(PathDeepScan, PathAttribute("foo"), PathIndices(Seq(0))))
      JsonPathParser.print(path) mustEqual raw
    }
    "correctly parse and print deep scan wildcards" in {
      val raw = "$..*"
      val path = JsonPathParser.parse(raw)
      path mustEqual JsonPath(Seq(PathDeepScan, PathAttributeWildCard))
      JsonPathParser.print(path) mustEqual raw
    }
    "correctly parse and print nested deep scans" in {
      val raw = "$.store..price" // the price of everything
      val path = JsonPathParser.parse(raw)
      path mustEqual JsonPath(Seq(PathAttribute("store"), PathDeepScan, PathAttribute("price")))
      JsonPathParser.print(path) mustEqual raw
    }
    "correctly parse and print functions" in {
      val raw = "$.foo.length()"
      val path = JsonPathParser.parse(raw)
      path mustEqual JsonPath(Seq(PathAttribute("foo")), Some(PathFunction.LengthFunction))
      JsonPathParser.print(path) mustEqual raw
    }
    "correctly parse and print filters on attribute exists" in {
      val raw = "$..book[?(@.isbn)]" // all books with an ISBN number
      val path = JsonPathParser.parse(raw)
      path mustEqual
        JsonPath(Seq(PathDeepScan, PathAttribute("book"),
          PathFilter(ExistsOp(PathExpression(JsonPath(Seq(PathAttribute("isbn"))))))))
      JsonPathParser.print(path) mustEqual raw
    }
    "correctly parse and print filters on less than number" in {
      val raw = "$.store.book[?(@.price < 10)]" // all books in store cheaper than 10
      val path = JsonPathParser.parse(raw)
      path mustEqual
        JsonPath(Seq(PathAttribute("store"), PathAttribute("book"),
          PathFilter(LessThanOp(PathExpression(JsonPath(Seq(PathAttribute("price")))), NumericLiteral(BigDecimal(10))))))
      JsonPathParser.print(path) mustEqual raw
    }
    "correctly parse and print comparison filters between two paths" in {
      val raw = "$..book[?(@.price <= $['expensive'])]" // all books in store that are not "expensive"
      val path = JsonPathParser.parse(raw)
      path mustEqual
        JsonPath(Seq(PathDeepScan, PathAttribute("book"),
          PathFilter(LessThanOrEqualsOp(PathExpression(JsonPath(Seq(PathAttribute("price")))),
            PathExpression(JsonPath(Seq(PathAttribute("expensive", bracketed = true))), absolute = true)))))
      JsonPathParser.print(path) mustEqual raw
    }
    "correctly parse and print regex filters" in {
      foreach(Seq(true, false)) { caseSensitive =>
        val flag = if (caseSensitive) { "" } else { "i" }
        val intFlag = if (caseSensitive) { 0 } else { Pattern.CASE_INSENSITIVE }
        val raw = "$..book[?(@.author =~ /.*REES/" + flag + ")]" // all books matching regex (ignore case)
        val path = JsonPathParser.parse(raw)
        path mustEqual
          JsonPath(Seq(PathDeepScan, PathAttribute("book"),
            PathFilter(RegexOp(PathExpression(JsonPath(Seq(PathAttribute("author")))), Pattern.compile(".*REES", intFlag)))))
        JsonPathParser.print(path) mustEqual raw
      }
    }
    "correctly parse and print not filters" in {
      val raw = "$..book[?(!@.isbn)]"
      val path = JsonPathParser.parse(raw)
      path mustEqual
        JsonPath(Seq(PathDeepScan, PathAttribute("book"),
          PathFilter(NotFilterOp(ExistsOp(PathExpression(JsonPath(Seq(PathAttribute("isbn")))))))))
      JsonPathParser.print(path) mustEqual raw
    }
    "correctly parse and print boolean filter combinations" in {
      val rawWithParens = "$..book[?((@.author =~ /.*REES/i && @.price <= $['expensive']) || @.price < 10)]"
      val rawImplicit = "$..book[?(@.author =~ /.*REES/i && @.price <= $['expensive'] || @.price < 10)]"
      foreach(Seq(rawWithParens, rawImplicit)) { raw =>
        val path = JsonPathParser.parse(raw)
        val regex = RegexOp(PathExpression(JsonPath(Seq(PathAttribute("author")))), Pattern.compile(".*REES", Pattern.CASE_INSENSITIVE))
        val lessThanPath =
          LessThanOrEqualsOp(PathExpression(JsonPath(Seq(PathAttribute("price")))),
            PathExpression(JsonPath(Seq(PathAttribute("expensive", bracketed = true))), absolute = true))
        val lessThanLit = LessThanOp(PathExpression(JsonPath(Seq(PathAttribute("price")))), NumericLiteral(BigDecimal(10)))
        path mustEqual
          JsonPath(Seq(PathDeepScan, PathAttribute("book"),
            PathFilter(OrFilterOp(Seq(AndFilterOp(Seq(regex, lessThanPath)), lessThanLit)))))
        // we insert parens around the implicit order of operations for &&
        JsonPathParser.print(path) mustEqual rawWithParens
      }
    }
    "correctly parse and print nested boolean filter combinations" in {
      val raw = "$..book[?(@.author =~ /.*REES/i && (@.price <= $['expensive'] || @.price < 10))]"
      val path = JsonPathParser.parse(raw)
      val regex = RegexOp(PathExpression(JsonPath(Seq(PathAttribute("author")))), Pattern.compile(".*REES", Pattern.CASE_INSENSITIVE))
      val lessThanPath =
        LessThanOrEqualsOp(PathExpression(JsonPath(Seq(PathAttribute("price")))),
          PathExpression(JsonPath(Seq(PathAttribute("expensive", bracketed = true))), absolute = true))
      val lessThanLit = LessThanOp(PathExpression(JsonPath(Seq(PathAttribute("price")))), NumericLiteral(BigDecimal(10)))
      path mustEqual
        JsonPath(Seq(PathDeepScan, PathAttribute("book"),
          PathFilter(AndFilterOp(Seq(regex, OrFilterOp(Seq(lessThanPath, lessThanLit)))))))
      JsonPathParser.print(path) mustEqual raw
    }
    "work with json-path examples" in {
      val tests = Seq(
        "$..book[2]"   -> JsonPath(Seq(PathDeepScan, PathAttribute("book"), PathIndices(Seq(2))), None),
        "$..book[-2]"  -> JsonPath(Seq(PathDeepScan, PathAttribute("book"), PathIndices(Seq(-2))), None),
        "$..book[0,1]" -> JsonPath(Seq(PathDeepScan, PathAttribute("book"), PathIndices(Seq(0, 1))), None),
        "$..book[:2]"  -> JsonPath(Seq(PathDeepScan, PathAttribute("book"), PathIndexRange(None, Some(2))), None),
        "$..book[1:2]" -> JsonPath(Seq(PathDeepScan, PathAttribute("book"), PathIndexRange(Some(1), Some(2))), None),
        "$..book[-2:]" -> JsonPath(Seq(PathDeepScan, PathAttribute("book"), PathIndexRange(Some(-2), None)), None),
        "$..book[2:]"  -> JsonPath(Seq(PathDeepScan, PathAttribute("book"), PathIndexRange(Some(2), None)), None),
        "$..book.length()" -> JsonPath(Seq(PathDeepScan, PathAttribute("book")), Some(PathFunction.LengthFunction)),
      )
      foreach(tests) { case (raw, expected) =>
        JsonPathParser.parse(raw) mustEqual expected
        JsonPathParser.print(expected) mustEqual raw
      }
    }
  }
}
