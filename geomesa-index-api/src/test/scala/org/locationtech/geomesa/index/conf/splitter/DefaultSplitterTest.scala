/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.conf.splitter

import com.google.common.primitives.Shorts
import org.junit.runner.RunWith
import org.locationtech.geomesa.curve.Z3SFC
import org.locationtech.geomesa.utils.geotools.SftBuilder
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DefaultSplitterTest extends Specification {

  "Default splitter" should {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    val sft =
      new SftBuilder()
        .point("geom", default = true)
        .date("dtg", default = true)
        .stringType("stringattr", index = true)
        .build("foo")

    val splitter = new DefaultSplitter

    "produce correct z3 splits" in {
      val opts = s"z3.min:2017-01-01,z3.max:2017-01-10,z3.bits:4"
      val splits = splitter.getSplits(sft, "z3", opts)
      val sfc = Z3SFC(sft.getZ3Interval)

      splits.length must be equalTo 32

      splits.toSeq.map(s => (Shorts.fromByteArray(s.take(2)).toInt, s(2).toInt, s.drop(3).sum.toInt)) must
          containTheSameElementsAs(Seq(2452, 2453).flatMap(w => Range(0, 128, 8).map((w, _, 0))))
    }

    "produce correct string splits" in {
      val opts = "attr.stringattr.pattern:[A-Z]"
      val splits = splitter.getSplits(sft, "attr", opts)
      splits.length must be equalTo 26
      new String(splits.head) must be equalTo "A"
    }

    "produce correct union string splits" in {
      val opts = "attr.stringattr.pattern:[A-Z0-9]"
      val splits = splitter.getSplits(sft, "attr", opts)
      splits.length must be equalTo 36
      new String(splits(27)) must be equalTo "1"
    }

    "produce correct tiered string splits" in {
      val opts = "attr.stringattr.pattern:[A-Z][A-Z]"
      val splits = splitter.getSplits(sft, "attr", opts)
      splits.length must be equalTo 26*26
      new String(splits(27)) must be equalTo "BB"
    }

    "produce correct aggregated string splits" in {
      val opts = "attr.stringattr.pattern:[0-9],attr.stringattr.pattern2:[8-8][0-9]"
      val splits = splitter.getSplits(sft, "attr", opts)
      splits.length must be equalTo 20
      splits.toSeq.map(new String(_)) must containTheSameElementsAs((0 to 9).map(_.toString) ++ (0 to 9).map(i => s"8$i"))
    }
  }
}
