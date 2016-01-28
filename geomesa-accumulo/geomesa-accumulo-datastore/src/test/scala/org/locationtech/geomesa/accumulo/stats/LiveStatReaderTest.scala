/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.stats

import org.apache.accumulo.core.client.ZooKeeperInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.security.Authorizations
import org.joda.time.Interval
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class LiveStatReaderTest extends Specification {

  sequential

  lazy val connector = new ZooKeeperInstance("mycloud", "zoo1,zoo2,zoo3")
                         .getConnector("root", new PasswordToken("password"))

  val table = "geomesa_catalog"
  val feature = "twitter"

  "StatReader" should {

    "query accumulo" in {

      skipped("Meant for integration")

      val reader = new QueryStatReader(connector, (fName: String) => s"${table}_${fName}_queries")

      val results = reader.query(feature, new Interval(0, System.currentTimeMillis()), new Authorizations())

      results.foreach(println)

      success
    }
  }

}
