/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.filters

import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.index.index.z2.Z2IndexKeySpace
import org.locationtech.geomesa.index.utils.ExplainNull
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class Z2FilterTest extends Specification {

  val sft = SimpleFeatureTypes.createType("z2FilterTest", "dtg:Date,*geom:Point:srid=4326")

  val filters = Seq(
    "bbox(geom,38,48,52,62)"
  ).map(ECQL.toFilter)

  val values = filters.map(Z2IndexKeySpace.getIndexValues(sft, _, ExplainNull))

  "Z2Filter" should {
    "serialize to and from bytes" in {
      forall(values) { value =>
        val filter = Z2Filter(value)
        val result = Z2Filter.deserializeFromBytes(Z2Filter.serializeToBytes(filter))
        result mustEqual filter
      }
    }
    "serialize to and from strings" in {
      forall(values) { value =>
        val filter = Z2Filter(value)
        val result = Z2Filter.deserializeFromStrings(Z2Filter.serializeToStrings(filter))
        result mustEqual filter
      }
    }
  }
}
