/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.text

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class StringSerializationTest extends Specification {

  "StringSerialization" should {
    "encode and decode single values to string" >> {
      val values = Map("foo" -> Seq("bar", "baz"))
      val encoded = StringSerialization.encodeSeqMap(values)
      StringSerialization.decodeSeqMap(encoded) mustEqual values
    }
    "encode and decode multiple values to string" >> {
      val values = Map("foo" -> Seq("bar", "baz"), "bar" -> Seq("foo", "baz"))
      val encoded = StringSerialization.encodeSeqMap(values)
      StringSerialization.decodeSeqMap(encoded) mustEqual values
    }
    "encode and decode escaped values to string" >> {
      val values = Map("foo" -> Seq("bar", "baz"), "bar:String" -> Seq("'\"],blerg", "blah", "", "test"))
      val encoded = StringSerialization.encodeSeqMap(values)
      StringSerialization.decodeSeqMap(encoded) mustEqual values
    }
  }
}
