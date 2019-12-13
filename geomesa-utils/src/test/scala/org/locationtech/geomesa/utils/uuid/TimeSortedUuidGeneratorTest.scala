/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.uuid

import java.util.UUID

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TimeSortedUuidGeneratorTest extends Specification {

  val time = 1435598908099L // System.currentTimeMillis()

  "TimeSortedUuidGenerator" should {
    "create uuids with correct formats" >> {
      val id = TimeSortedUuidGenerator.createUuid(time).toString
      id.substring(0, 18) mustEqual "000014e4-05ce-4ac3"
      val uuid = UUID.fromString(id)
      uuid.version() mustEqual 4
      uuid.variant() mustEqual 2
    }
    "create uuids with time as the msb" >> {
      val ids = Seq(time - 1, time, time + 1, time + 1000)
          .map(TimeSortedUuidGenerator.createUuid).map(_.toString)
      ids.sorted mustEqual ids
    }
  }
}
