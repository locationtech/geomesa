/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.utils

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ExplainerTest extends Specification {

  "ExplainLogger" should {
    "lazily evaluate logged messages" in {
      // note: this assumes that log configuration is not set to trace level
      var count = 0
      val explain = new ExplainLogging
      explain({ count += 1; "foo" })
      explain.output({ count += 1; "foo" })
      explain.pushLevel({ count += 1; "foo" })
      explain.pushLevel()
      explain.popLevel({ count += 1; "foo" })
      explain.popLevel()
      explain({ count += 1; "foo" }, Seq(() => { count += 1; "foo" }, () => { count += 1; "foo" }))
      count mustEqual 0
    }
  }
}
