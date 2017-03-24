/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.jobs.accumulo

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AccumuloJobUtilsTest extends Specification {

  "AccumuloJobUtils" should {
    "load list of jars from class resource" in {
      AccumuloJobUtils.defaultLibJars must not(beNull)
      AccumuloJobUtils.defaultLibJars must not(beEmpty)
      AccumuloJobUtils.defaultLibJars must contain("accumulo")
      AccumuloJobUtils.defaultLibJars must contain("libthrift")
    }
  }
}
