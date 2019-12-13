/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.conf

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ConfigLoaderTest extends Specification {

  "ConfigLoader" should {
    "load config files" in {
      val config = ConfigLoader.loadConfig("geomesa-fake.xml")
      config.get("geomesa.config.test1") must beSome(("1", false))
      config.get("geomesa.config.test2") must beSome(("2", true)) // Final
      config.get("geomesa.config.test3") must beNone
    }
  }
}
