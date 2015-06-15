/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.jobs

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class JobUtilsTest extends Specification {

  "JobUtils" should {
    val testFolder = new File(getClass().getClassLoader.getResource("fakejars").getFile)

    "load list of jars from class resource" in {
      JobUtils.defaultLibJars must not beNull;
      JobUtils.defaultLibJars.isEmpty mustEqual(false)
      JobUtils.defaultLibJars must contain("accumulo-core")
    }

    "load jars from folder" in {
      val files = JobUtils.loadJarsFromFolder(testFolder)
      files.length mustEqual(3)
      files.map(_.getName) must contain("jar1.jar", "jar2.jar", "jar3.jar")
    }

    "load jars from classpath" in {
      val files = JobUtils.getJarsFromClasspath(classOf[JobUtilsTest])
      files.length must beGreaterThan(0)
    }

    "configure libjars based on search paths" in {
      val conf = new Configuration()
      val search = Seq("jar1", "jar3")
      val paths = Iterator(() => JobUtils.loadJarsFromFolder(testFolder))
      JobUtils.setLibJars(conf, search, paths)
      val libjars = conf.get("tmpjars")
      libjars must contain("fakejars/jar1.jar")
      libjars must contain("fakejars/nested/jar3.jar")
    }
  }

}
