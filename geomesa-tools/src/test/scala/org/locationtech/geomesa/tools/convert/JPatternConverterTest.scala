/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.convert

import com.beust.jcommander.JCommander
import org.junit.runner.RunWith
import org.locationtech.geomesa.tools.OptionalPatternParam
import org.locationtech.geomesa.tools.convert.JPatternConverterTest.PatternParam
import org.locationtech.geomesa.tools.utils.GeoMesaIStringConverterFactory
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class JPatternConverterTest extends Specification {

  "JPatternConverter" should {
    "convert strings into patterns" in {
      val params = new PatternParam()
      JCommander.newBuilder()
          .addConverterFactory(new GeoMesaIStringConverterFactory)
          .addObject(params)
          .build()
          .parse(Array("--pattern", "foobar\\d+"): _*)
      params.pattern.pattern() mustEqual "foobar\\d+"
      params.pattern.matcher("foobar3").matches mustEqual true
    }

    "allow nulls" in {
      val params = new PatternParam()
      JCommander.newBuilder()
          .addConverterFactory(new GeoMesaIStringConverterFactory)
          .addObject(params)
          .build()
          .parse()
      params.pattern must beNull
    }
  }
}

object JPatternConverterTest {
  class PatternParam extends OptionalPatternParam {}
}
