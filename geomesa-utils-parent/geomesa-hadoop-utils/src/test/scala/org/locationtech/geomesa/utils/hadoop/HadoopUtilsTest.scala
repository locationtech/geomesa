/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

<<<<<<< HEAD:geomesa-utils-parent/geomesa-hadoop-utils/src/test/scala/org/locationtech/geomesa/utils/hadoop/HadoopUtilsTest.scala
package org.locationtech.geomesa.utils.hadoop
=======
package org.locationtech.geomesa.utils.io
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support):geomesa-utils/src/test/scala/org/locationtech/geomesa/utils/io/HadoopUtilsTest.scala
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support):geomesa-utils/src/test/scala/org/locationtech/geomesa/utils/io/HadoopUtilsTest.scala
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support):geomesa-utils/src/test/scala/org/locationtech/geomesa/utils/io/HadoopUtilsTest.scala
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.io.File

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
