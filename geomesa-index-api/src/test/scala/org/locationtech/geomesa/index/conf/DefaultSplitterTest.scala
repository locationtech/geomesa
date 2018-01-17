/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.conf

import com.google.common.primitives.Shorts
import org.junit.runner.RunWith
import org.locationtech.geomesa.curve.Z3SFC
import org.locationtech.geomesa.index.conf.splitter.DefaultSplitter
import org.locationtech.geomesa.utils.geotools.SftBuilder
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DefaultSplitterTest extends Specification {

  "Default splitter" should {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    import scala.collection.JavaConversions._

    val sft =
      new SftBuilder()
        .point("geom", default = true)
        .date("dtg", default = true)
        .stringType("stringattr", index = true)
        .build("foo")

    val splitter = new DefaultSplitter

    "produce correct z3 splits" in {
      val min = "2017-01-01T00:00:00.000Z"
      val max = "2017-01-10T23:59:59.999Z"
      val bits = 4
      val opts = Map("z3.min" -> min, "z3.max" -> max, "z3.bits" -> s"$bits")
      val splits = splitter.getSplits("z3", sft, opts)
      val sfc = Z3SFC(sft.getZ3Interval)

      splits.length must be equalTo 32

      splits.toSeq.map(s => (Shorts.fromByteArray(s.take(2)).toInt, s(2).toInt, s.drop(3).sum.toInt)) must
          containTheSameElementsAs(Seq(2452, 2453).flatMap(w => Range(0, 128, 8).map((w, _, 0))))
    }

    "produce correct string splits" in {
      val splits = splitter.getSplits("attr", sft, Map("attr.stringattr.pattern" -> "[A-Z]"))
      splits.length must be equalTo 26
      new String(splits.head) must be equalTo "A"
    }

    "produce correct string splits multi" in {
      val splits = splitter.getSplits("attr", sft, Map("attr.stringattr.pattern" -> "[A-Z][A-Z]"))
      splits.length must be equalTo 26*26
      new String(splits(27)) must be equalTo "BB"
    }
  }
}
