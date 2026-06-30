/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.parquet.schema

import org.locationtech.geomesa.fs.storage.core.schema.ColumnName
import org.specs2.mutable.SpecificationWithJUnit

class ColumnNameTest extends SpecificationWithJUnit {

  val cases = Seq(
    "fooBar123" -> "foobar123",
    "foo_bar" -> "foo_bar",
    "foo_bar_baz" -> "foo_bar_baz",
    "foo__bar" -> "foo_bar", // double underscores
    "foo___bar" -> "foo_bar", // triple underscores
    "__foo" -> "__foo", // leading double underscores get ignored
    "foo__" -> "foo_", // trailing double underscores
    "foo-bar" -> "foo_bar", // dash
    "foo bar" -> "foo_bar", // spaces
    "foo.bar" -> "foo_bar", // dot
    "foo-bar.baz" -> "foo_bar_baz", // mixed non-alpha
    "foo(bar)" -> "foo_bar_", // parens
    "foo_bar-baz" -> "foo_bar_baz", // mixed underscores and non-alpha
    "foo\u00e9bar" -> "foo_bar", // unicode
  )

  "ColumnName" should {
    "encode names" in {
      foreach(cases) { case (original, encoded) =>
        val col = ColumnName(original)
        col.attribute mustEqual original
        col.column mustEqual encoded
        ColumnName.encode(original) mustEqual encoded
      }
    }
  }
}
