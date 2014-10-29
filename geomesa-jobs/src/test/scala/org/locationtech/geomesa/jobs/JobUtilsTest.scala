/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
