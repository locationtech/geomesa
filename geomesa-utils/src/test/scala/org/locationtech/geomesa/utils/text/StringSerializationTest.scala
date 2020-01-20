/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.text

import java.time.format.DateTimeFormatter
import java.time.{ZoneOffset, ZonedDateTime}
import java.util.Date

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class StringSerializationTest extends Specification {

  def stringMap: Map[String, Class[_]] = Seq("foo", "bar", "bar:String").map(_ -> classOf[String]).toMap

  "StringSerialization" should {

    "encode and decode string seqs" >> {
      "with empty values" >> {
        StringSerialization.encodeSeq(Seq.empty) mustEqual ""
        StringSerialization.decodeSeq("") must beEmpty
      }
      "with a single value" >> {
        val values = Seq("foo")
        val encoded = StringSerialization.encodeSeq(values)
        StringSerialization.decodeSeq(encoded) mustEqual values
      }
      "with multiple values" >> {
        val values = Seq("foo", "bar", "baz")
        val encoded = StringSerialization.encodeSeq(values)
        StringSerialization.decodeSeq(encoded) mustEqual values
      }
      "with escaped values" >> {
        val values = Seq("foo", "bar:String", "'\"],blerg", "baz", "")
        val encoded = StringSerialization.encodeSeq(values)
        StringSerialization.decodeSeq(encoded) mustEqual values
      }
    }

    "encode and decode string maps" >> {
      "with empty values" >> {
        StringSerialization.encodeMap(Map.empty) mustEqual ""
        StringSerialization.decodeMap("") must beEmpty
      }
      "with a single value" >> {
        val values = Map("foo" -> "bar")
        val encoded = StringSerialization.encodeMap(values)
        StringSerialization.decodeMap(encoded) mustEqual values
      }
      "with multiple values" >> {
        val values = Map("foo" -> "bar", "bar" -> "foo")
        val encoded = StringSerialization.encodeMap(values)
        StringSerialization.decodeMap(encoded) mustEqual values
      }
      "with escaped values" >> {
        val values = Map("foo" -> "bar:String", "bar" -> "'\"],blerg", "baz" -> "")
        val encoded = StringSerialization.encodeMap(values)
        StringSerialization.decodeMap(encoded) mustEqual values
      }
    }

    "encode and decode seq maps" >> {
      "with empty values" >> {
        StringSerialization.encodeSeqMap(Map.empty) mustEqual ""
        StringSerialization.decodeSeqMap("", Map.empty[String, Class[_]]) must beEmpty
      }
      "with a single value" >> {
        val values = Map("foo" -> Seq("bar", "baz"))
        val encoded = StringSerialization.encodeSeqMap(values)
        StringSerialization.decodeSeqMap(encoded, stringMap).mapValues(_.toSeq) mustEqual values
      }
      "with multiple values" >> {
        val values = Map("foo" -> Seq("bar", "baz"), "bar" -> Seq("foo", "baz"))
        val encoded = StringSerialization.encodeSeqMap(values)
        StringSerialization.decodeSeqMap(encoded, stringMap).mapValues(_.toSeq) mustEqual values
      }
      "with escaped values" >> {
        val values = Map("foo" -> Seq("bar", "baz"), "bar:String" -> Seq("'\"],blerg", "blah", "", "test"))
        val encoded = StringSerialization.encodeSeqMap(values)
        StringSerialization.decodeSeqMap(encoded, stringMap).mapValues(_.toSeq) mustEqual values
      }
      "with non-string values" >> {
        val dt = DateTimeFormatter.ISO_OFFSET_DATE_TIME.withZone(ZoneOffset.UTC)
        val dates = Seq("2017-01-01T00:00:00.000Z", "2017-01-01T01:00:00.000Z").map(d => Date.from(ZonedDateTime.parse(d, dt).toInstant))
        val values = Map("dtg" -> dates, "age" -> Seq(0, 1, 2).map(Int.box), "height" -> Seq(0.1f, 0.2f, 0.5f).map(Float.box))
        val encoded = StringSerialization.encodeSeqMap(values)
        val bindings = Map("dtg" -> classOf[Date], "age" -> classOf[Integer], "height" -> classOf[java.lang.Float])
        StringSerialization.decodeSeqMap(encoded, bindings).mapValues(_.toSeq) mustEqual values
      }
    }
  }
}
