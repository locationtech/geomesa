/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.utils

import com.beust.jcommander.{JCommander, Parameter}
import org.junit.runner.RunWith
import org.locationtech.geomesa.tools.utils.ParameterConverters.DurationConverter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.concurrent.duration.Duration

@RunWith(classOf[JUnitRunner])
class ParameterConvertersTest extends Specification {

  "ParameterConverters" should {
    "parse durations" in {
      val params = new AnyRef {
        @Parameter(names = Array("-d"), description = "duration", converter = classOf[DurationConverter])
        var duration: Duration = _
      }

      val jc = new JCommander()
      jc.setProgramName("test")
      jc.addCommand("foo", params)
      jc.parse("foo", "-d", "5 SECONDS")

      jc.getParsedCommand mustEqual "foo"
      params.duration mustEqual Duration("5 seconds")
    }
  }
}
