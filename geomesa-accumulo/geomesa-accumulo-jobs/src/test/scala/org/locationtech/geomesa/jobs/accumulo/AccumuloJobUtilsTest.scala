/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.jobs.accumulo

import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.jobs.AccumuloJobUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AccumuloJobUtilsTest extends Specification {

  "AccumuloJobUtils" should {
    "load list of jars from class resource" in {
      AccumuloJobUtils.defaultLibJars must not(beNull)
      AccumuloJobUtils.defaultLibJars must not(beEmpty)
      AccumuloJobUtils.defaultLibJars must contain("accumulo-core")
      AccumuloJobUtils.defaultLibJars must contain("libthrift")
    }
  }
}
