/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.jobs

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.classpath.ClassPathUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class JobUtilsTest extends Specification {

  "JobUtils" should {
    "configure libjars based on search paths" in {
      val testFolder = new File(getClass.getClassLoader.getResource("fakejars").getFile)
      val conf = new Configuration()
      val search = Seq("jar1", "jar3")
      val paths = Iterator(() => ClassPathUtils.loadJarsFromFolder(testFolder))
      JobUtils.setLibJars(conf, search, paths)
      val libjars = conf.get("tmpjars")
      libjars must contain("fakejars/jar1.jar")
      libjars must contain("fakejars/nested/jar3.jar")
    }
  }
}
