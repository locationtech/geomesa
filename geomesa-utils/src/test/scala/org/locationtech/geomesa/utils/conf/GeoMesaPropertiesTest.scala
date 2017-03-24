/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.conf

import com.typesafe.scalalogging.LazyLogging
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GeoMesaPropertiesTest extends Specification with LazyLogging {

  sequential

  val TEST_PROP_1 = "test.system.properties.1"
  val TEST_PROP_2 = "test.system.properties.2"
  val TEST_PROP_3 = "test.system.properties.3"
  val REAL_PROP = "geomesa.stats.compact.millis"
  val REAL_PROP_VAL = "3600000"

  def testProp1 = SystemProperty(TEST_PROP_1)
  def testProp2 = SystemProperty(TEST_PROP_2, "default")
  // This is loaded from embedded config
  def realProp = SystemProperty(REAL_PROP)

  "props" should {
    "contain system properties" in {
      GeoMesaProperties.ProjectVersion must not beNull;
      GeoMesaProperties.BuildDate must not beNull;
      GeoMesaProperties.GitCommit must not beNull;
      GeoMesaProperties.GitBranch must not beNull;
    }
  }

  "GeoMesaSystemProperty" should {
    "return proper values" in {
      testProp1.default must beNull
      testProp1.get must beNull
      testProp1.option must beEqualTo(None)

      System.setProperty(testProp2.property, "test")
      testProp2.get must beEqualTo("test")
      testProp2.option must beEqualTo(Option("test"))
      System.clearProperty(testProp2.property)
      testProp2.get must beEqualTo("default")

      realProp.default must beNull
      realProp.get must beEqualTo(REAL_PROP_VAL)
      realProp.option must beEqualTo(Option(REAL_PROP_VAL))
    }
  }

  "getProperty" should {
    "return null when property is empty" in {
      GeoMesaSystemProperties.getProperty(TEST_PROP_3) must beNull
    }

    "return proper values" in {
      System.setProperty(TEST_PROP_3, "test")
      GeoMesaSystemProperties.getProperty(TEST_PROP_3) must beEqualTo("test")
    }
  }
}
