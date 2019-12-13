/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
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

@RunWith(classOf[JUnitRunner])
class JsonPathParserTest extends Specification {

  "JsonPathParser" should {
    "not parse invalid paths" in {
      JsonPathParser.parse("$.$") must throwA[ParsingException]
      JsonPathParser.parse("$.foo foo") must throwA[ParsingException]
    }
    "correctly parse attribute paths" in {
      val path = JsonPathParser.parse("$.foo")
      path must haveLength(1)
      path.head mustEqual PathAttribute("foo")
    }
    "correctly parse attribute bracket paths" in {
      JsonPathParser.parse("$[foo]") mustEqual Seq(PathAttribute("foo", bracketed = true))
      JsonPathParser.parse("$[foo_bar]") mustEqual Seq(PathAttribute("foo_bar", bracketed = true))
      JsonPathParser.parse("$['foo']") mustEqual Seq(PathAttribute("foo", bracketed = true))
      JsonPathParser.parse("$['foo_bar']") mustEqual Seq(PathAttribute("foo_bar", bracketed = true))
      JsonPathParser.parse("$['foo-bar 0']") mustEqual Seq(PathAttribute("foo-bar 0", bracketed = true))
    }
    "correctly parse array index paths" in {
      val path = JsonPathParser.parse("$.foo[2]")
      path must haveLength(2)
      path mustEqual Seq(PathAttribute("foo"), PathIndex(2))
    }
    "correctly parse multiple array index paths" in {
      val path = JsonPathParser.parse("$.foo[2,3,4]")
      path must haveLength(2)
      path mustEqual Seq(PathAttribute("foo"), PathIndices(Seq(2, 3, 4)))
    }
    "correctly parse array index range paths" in {
      val path = JsonPathParser.parse("$.foo[2:4]")
      path must haveLength(2)
      path mustEqual Seq(PathAttribute("foo"), PathIndices(Seq(2, 3)))
    }
    "correctly parse wildcards in attribute paths" in {
      val path = JsonPathParser.parse("$.foo.*.name")
      path must haveLength(3)
      path mustEqual Seq(PathAttribute("foo"), PathAttributeWildCard, PathAttribute("name"))
    }
    "correctly parse wildcards in array index paths" in {
      val path = JsonPathParser.parse("$.foo[*]")
      path must haveLength(2)
      path mustEqual Seq(PathAttribute("foo"), PathIndexWildCard)
    }
    "correctly parse deep scan attributes" in {
      val path = JsonPathParser.parse("$..foo[0]")
      path must haveLength(3)
      path mustEqual Seq(PathDeepScan, PathAttribute("foo"), PathIndex(0))
    }
    "correctly parse deep scan wildcards" in {
      val path = JsonPathParser.parse("$..*")
      path must haveLength(2)
      path mustEqual Seq(PathDeepScan, PathAttributeWildCard)
    }
    "correctly parse nested deep scans" in {
      val path = JsonPathParser.parse("$.foo..bar")
      path must haveLength(3)
      path mustEqual Seq(PathAttribute("foo"), PathDeepScan, PathAttribute("bar"))
    }
    "correctly parse functions" in {
      val path = JsonPathParser.parse("$.foo.length()")
      path must haveLength(2)
      path mustEqual Seq(PathAttribute("foo"), PathFunction(JsonPathFunction.length))
    }
  }
}
