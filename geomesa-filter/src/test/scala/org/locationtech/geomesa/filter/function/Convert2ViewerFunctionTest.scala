/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.filter.function

import java.util.Date

import org.geotools.data.Base64
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder.EncodedValues
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class Convert2ViewerFunctionTest extends Specification {

  import org.locationtech.geomesa.filter.ff

  "Convert2ViewerFunction" should {
    "convert inputs" in {
      import scala.collection.JavaConversions._
      val sft = SimpleFeatureTypes.createType("foo", "foo:String,dtg:Date,*geom:Point:srid=4326")
      val sf = ScalaSimpleFeature.create(sft, "", "foo", "2017-01-01T00:00:00.000Z", "POINT (45 50)")
      val fn = new Convert2ViewerFunction()
      fn.setParameters(List(ff.property("foo"), ff.property("geom"), ff.property("dtg")))
      val result = Base64.decode(fn.evaluate(sf))
      result must haveLength(24)
      BinaryOutputEncoder.decode(result) mustEqual
          EncodedValues(
            BinaryOutputEncoder.convertToTrack("foo"),
            50f, 45f,
            BinaryOutputEncoder.convertToDate(sf.getAttribute("dtg").asInstanceOf[Date]),
            BinaryOutputEncoder.convertToLabel("foo"))
    }

  }
}