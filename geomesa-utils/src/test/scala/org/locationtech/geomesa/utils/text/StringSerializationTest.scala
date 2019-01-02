/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.text

import java.time.{ZoneOffset, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.util.Date

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class StringSerializationTest extends Specification {

  "StringSerialization" should {
    "encode and decode single values to string" >> {
      val values = Map("foo" -> Seq("bar", "baz"))
      val encoded = StringSerialization.encodeSeqMap(values)
      StringSerialization.decodeSeqMap(encoded, Map.empty[String, Class[_]]).mapValues(_.toSeq) mustEqual values
    }
    "encode and decode multiple values to string" >> {
      val values = Map("foo" -> Seq("bar", "baz"), "bar" -> Seq("foo", "baz"))
      val encoded = StringSerialization.encodeSeqMap(values)
      StringSerialization.decodeSeqMap(encoded, Map.empty[String, Class[_]]).mapValues(_.toSeq) mustEqual values
    }
    "encode and decode escaped values to string" >> {
      val values = Map("foo" -> Seq("bar", "baz"), "bar:String" -> Seq("'\"],blerg", "blah", "", "test"))
      val encoded = StringSerialization.encodeSeqMap(values)
      StringSerialization.decodeSeqMap(encoded, Map.empty[String, Class[_]]).mapValues(_.toSeq) mustEqual values
    }
    "encode and decode non-string values" >> {
      val dt = DateTimeFormatter.ISO_OFFSET_DATE_TIME.withZone(ZoneOffset.UTC)
      val dates = Seq("2017-01-01T00:00:00.000Z", "2017-01-01T01:00:00.000Z").map(d => Date.from(ZonedDateTime.parse(d, dt).toInstant))
      val values = Map("dtg" -> dates, "age" -> Seq(0, 1, 2).map(Int.box), "height" -> Seq(0.1f, 0.2f, 0.5f).map(Float.box))
      val encoded = StringSerialization.encodeSeqMap(values)
      val bindings = Map("dtg" -> classOf[Date], "age" -> classOf[Integer], "height" -> classOf[java.lang.Float])
      StringSerialization.decodeSeqMap(encoded, bindings).mapValues(_.toSeq) mustEqual values
    }
  }
}
