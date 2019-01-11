/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.jobs

import org.apache.hadoop.conf.Configuration
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GeoMesaConfiguratorTest extends Specification {

  "GeoMesaConfigurator" should {
    "set and retrieve data store params" in {
      val in  = Map("user" -> "myuser", "password" -> "mypassword", "instance" -> "myinstance")
      val out = Map("user" -> "myuser2", "password" -> "mypassword2", "instance" -> "myinstance2")
      val conf = new Configuration()
      GeoMesaConfigurator.setDataStoreInParams(conf, in)
      GeoMesaConfigurator.setDataStoreOutParams(conf, out)
      val recoveredIn = GeoMesaConfigurator.getDataStoreInParams(conf)
      val recoveredOut = GeoMesaConfigurator.getDataStoreOutParams(conf)
      recoveredIn mustEqual(in)
      recoveredOut mustEqual(out)
    }
  }

}
