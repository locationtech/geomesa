/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.stats

import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class StatTest extends Specification with StatTestHelper {

  "Stat parser" should {
    "fail for malformed strings" in {
      Stat(sft, "") must throwAn[Exception]
      Stat(sft, "abcd") must throwAn[Exception]
      Stat(sft, "RangeHistogram()") must throwAn[Exception]
      Stat(sft, "RangeHistogram(foo,10,2012-01-01T00:00:00.000Z,2012-02-01T00:00:00.000Z)") must throwAn[Exception]
      Stat(sft, "MinMax()") must throwAn[Exception]
      Stat(sft, "MinMax(abcd)") must throwAn[Exception]
    }
    "work with complex values" in {
      val sft = SimpleFeatureTypes.createType("foo", "foreign_key:String,*wkt:Point:srid=4326")
      val min = """geoMesaNameSpace\u001Fgeokind\u001FtDZdPCceQg+F\/a0tV0azJA==0"""
      val max = """geoMesaNameSpace\u001Fgeokind\u001FtDZdPCceQg+F\/a0tV0azJA==z"""
      val stat = Stat(sft, Stat.Histogram("foreign_key", 1000, min, max))
      stat must beAnInstanceOf[Histogram[String]]
      val histogram = stat.asInstanceOf[Histogram[String]]
      histogram.property mustEqual "foreign_key"
      histogram.bins.bounds mustEqual BinnedStringArray.normalizeBounds(min, max)
    }
  }
}
