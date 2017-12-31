/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.conf

import com.google.common.primitives.Shorts
import org.junit.runner.RunWith
import org.locationtech.geomesa.curve.NormalizedDimension.{NormalizedLat, NormalizedLon}
import org.locationtech.geomesa.curve.Z3SFC
import org.locationtech.geomesa.index.conf.splitter.DefaultSplitter
import org.locationtech.geomesa.index.filters.Z3Filter
import org.locationtech.geomesa.utils.geotools.SftBuilder
import org.locationtech.sfcurve.zorder.Z3
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
      val max = "2017-01-31T23:59:59.999Z"
      val bits = 4
      val opts = Map("geom.type" -> "z3", "min" -> min, "max" -> max, "bits" -> s"$bits")
      val splits = splitter.getSplits("geom", sft, opts)
      val sfc = Z3SFC(sft.getZ3Interval)

      splits.length must be equalTo 40

      val lat = NormalizedLat(21)
      val lon = NormalizedLon(21)

      splits.foreach { s =>
        val e = Shorts.fromByteArray(s.take(2))
        val z = new Z3(Z3Filter.rowToZ(s, 0))
        val (x, y, t) = z.decode

        println(s"$e   $t   ${lon.denormalize(x)}  ${lat.denormalize(y)}")
      }

      // TODO: check that the z3 splits are correct
      true must beTrue
    }

    "produce correct string splits" in {
      val splits = splitter.getSplits("stringattr", sft,
        Map("stringattr.type"    -> "attribute"
          , "stringattr.pattern" -> "[:upper:]"))

      splits.length must be equalTo 26
      new String(splits.head) must be equalTo "A"
    }

    "produce correct string splits multi" in {
      val splits = splitter.getSplits("stringattr", sft,
        Map("stringattr.type"    -> "attribute"
          , "stringattr.pattern" -> "[:upper:][:upper:]"))

      splits.length must be equalTo 26*26
      new String(splits(27)) must be equalTo "AB"
    }
  }
}
