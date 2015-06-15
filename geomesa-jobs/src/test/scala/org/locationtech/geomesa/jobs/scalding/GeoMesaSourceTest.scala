/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.jobs.scalding

import com.twitter.scalding.{Hdfs, Read, Write}
import org.apache.hadoop.conf.Configuration
import org.junit.runner.RunWith
import org.locationtech.geomesa.jobs.scalding.taps.GeoMesaTap
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GeoMesaSourceTest extends Specification {

  val sftName = "GeoMesaSourceTest"
  val params = Map(
    "instanceId" -> "GeoMesaSourceTest",
    "zookeepers" -> "zoo",
    "tableName"  -> "GeoMesaSourceTest",
    "user"       -> "user",
    "password"   -> "password",
    "useMock"    -> "true"
  )

  val input = GeoMesaInputOptions(params, sftName, None)
  val output = GeoMesaOutputOptions(params)

  "GeoMesaSource" should {
    "create read and write taps" in {
      implicit val mode = Hdfs(true, new Configuration())
      val readTap = GeoMesaSource(input).createTap(Read)
      val writeTap = GeoMesaSource(output).createTap(Write)
      readTap must haveClass[GeoMesaTap]
      writeTap must haveClass[GeoMesaTap]
      readTap.getIdentifier mustNotEqual(writeTap.getIdentifier)
    }
  }
}
