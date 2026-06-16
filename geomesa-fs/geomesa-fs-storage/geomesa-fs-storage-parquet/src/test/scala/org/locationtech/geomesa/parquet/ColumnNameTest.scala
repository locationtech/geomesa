/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.parquet

import org.locationtech.geomesa.fs.storage.parquet.io.ColumnName
import org.specs2.mutable.SpecificationWithJUnit

class ColumnNameTest extends SpecificationWithJUnit {

  val cases = Seq(
    "fooBar123" -> "fooBar123",
    "foo_bar" -> "foo_bar",
    "foo_bar_baz" -> "foo_bar_baz",
    "foo__bar" -> "foo__5f__5fbar", // double underscores
    "foo___bar" -> "foo__5f__5f__5fbar", // triple underscores
    "__foo" -> "__foo", // leading double underscores get ignored
    "foo__" -> "foo__5f__5f", // trailing double underscores
    "foo-bar" -> "foo__2dbar", // dash
    "foo bar" -> "foo__20bar", // spaces
    "foo.bar" -> "foo__2ebar", // dot
    "foo-bar.baz" -> "foo__2dbar__2ebaz", // mixed non-alpha
    "foo(bar)" -> "foo__28bar__29", // parens
    "foo_bar-baz" -> "foo_bar__2dbaz", // mixed underscores and non-alpha
    "foo\u00e9bar" -> "foo__c3__a9bar", // unicode
  )

  val roundTrips = Seq(
    "simple",
    "foo_bar",
    "foo__bar",
    "foo___bar",
    "foo-bar",
    "foo bar",
    "foo.bar",
    "foo(bar)",
    "foo_bar-baz",
    "__foo",
    "foo__",
    "_foo_",
    "foo\u00e9bar",
    "a1_b2_c3",
    "test__double__under"
  )

  "ColumnName" should {
    "encode and decode names" in {
      foreach(cases) { case (original, encoded) =>
        ColumnName(original) mustEqual encoded
        val ColumnName(decoded) = encoded
        decoded mustEqual original
      }
    }

    "round-trip encode and decode" in {
      foreach(roundTrips) { name =>
        val encoded = ColumnName(name)
        val ColumnName(decoded) = encoded
        decoded mustEqual name
      }
    }

    "handle edge case with invalid hex patterns" in {
      foreach(Seq("foo__zzbar", "foo__2")) { invalid =>
        val ColumnName(decoded) = invalid
        decoded mustEqual invalid
      }
    }
  }
}
