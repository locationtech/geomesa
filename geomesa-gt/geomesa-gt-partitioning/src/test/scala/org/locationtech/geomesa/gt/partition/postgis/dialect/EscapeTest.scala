/***********************************************************************
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class EscapeTest extends Specification {

  "PartitionedPostgisDialect" should {
    "Escape literal values" in {
      SqlLiteral("foo'bar").raw mustEqual "foo'bar"
      SqlLiteral("foo'bar").quoted mustEqual "'foo''bar'"
      SqlLiteral("foo\"bar").quoted mustEqual "'foo\"bar'"
    }
    "Escape identifiers" in {
      FunctionName("foo'bar").raw mustEqual "foo'bar"
      FunctionName("foo'bar").quoted mustEqual "\"foo'bar\""
      FunctionName("foo\"bar").quoted mustEqual "\"foo\"\"bar\""
    }
  }
}
