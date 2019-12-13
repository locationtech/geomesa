/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.convert

import com.beust.jcommander.JCommander
import org.junit.runner.RunWith
import org.locationtech.geomesa.tools.OptionalPatternParam
import org.locationtech.geomesa.tools.utils.GeoMesaIStringConverterFactory
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

class PatternParam extends OptionalPatternParam {}

@RunWith(classOf[JUnitRunner])
class JPatternConverterTest extends Specification {

  "JPatternConverter" should {
    "convert strings into patterns" in {
      val params = new PatternParam()
      val jc = new JCommander()
      jc.addConverterFactory(new GeoMesaIStringConverterFactory)
      jc.addObject(params)
      jc.parse(Array("--pattern", "foobar\\d+").toArray: _*)
      params.pattern.pattern() mustEqual "foobar\\d+"
      params.pattern.matcher("foobar3").matches mustEqual true
    }

    "allow nulls" in {
      val params = new PatternParam()
      val jc = new JCommander()
      jc.addConverterFactory(new GeoMesaIStringConverterFactory)
      jc.addObject(params)
      jc.parse(Array("").toArray: _*)
      params.pattern must beNull
    }
  }
}
