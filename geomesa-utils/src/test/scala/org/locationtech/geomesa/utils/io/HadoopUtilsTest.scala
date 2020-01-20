/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.io

import java.io.File

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class HadoopUtilsTest extends Specification with LazyLogging {

  "HadoopUtils" should {
    "add resources to a conf" in {
      val conf = new Configuration(false)
      val resource = new File(getClass.getClassLoader.getResource("geomesa-fake.xml").toURI).getAbsolutePath
      HadoopUtils.addResource(conf, resource)
      conf.get("geomesa.config.test1") mustEqual "1"
    }
  }
}


