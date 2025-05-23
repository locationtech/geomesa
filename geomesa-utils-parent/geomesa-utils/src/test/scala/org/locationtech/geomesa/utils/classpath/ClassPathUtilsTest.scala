/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.classpath


import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.io.File

@RunWith(classOf[JUnitRunner])
class ClassPathUtilsTest extends Specification {
  "JobUtils" should {
    val testFolder = new File(getClass.getClassLoader.getResource("fakejars").getFile)

    "load jars from folder" in {
      val files = ClassPathUtils.loadJarsFromFolder(testFolder)
      files must haveLength(3)
      files.map(_.getName) must contain("jar1.jar", "jar2.jar", "jar3.jar")
    }

    "load jars from classpath" in {
      // TODO
      skipped("fails in java 11")
      val files = ClassPathUtils.getJarsFromClasspath(classOf[ClassPathUtilsTest])
      files.length must beGreaterThan(0)
    }
  }

}
