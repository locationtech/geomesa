/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.utils.conf

import java.io.File

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ConfigLoaderTest extends Specification {

  "loadConfig" should {
    val testConfig = getClass.getClassLoader.getResource("geomesa-fake.xml").getPath

    System.setProperty("geomesa.config.test1", "a")
    System.setProperty("geomesa.config.test2", "b")

    "set system properties" in {
      ConfigLoader.loadConfig(testConfig)
      System.getProperty("geomesa.config.test1") must beEqualTo("a")
      System.getProperty("geomesa.config.test2") must beEqualTo("2") // Final
      ConfigLoader.isLoaded must beTrue
    }
  }
}
