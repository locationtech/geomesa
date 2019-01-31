/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.filters

import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.index.api.ShardStrategy.NoShardStrategy
import org.locationtech.geomesa.index.index.z2.Z2IndexKeySpace
import org.locationtech.geomesa.index.utils.ExplainNull
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class Z2FilterTest extends Specification {

  val sft = SimpleFeatureTypes.createType("z2FilterTest", "dtg:Date,*geom:Point:srid=4326")

  val keySpace = new Z2IndexKeySpace(sft, NoShardStrategy, "geom")

  val filters = Seq(
    "bbox(geom,38,48,52,62)"
  ).map(ECQL.toFilter)

  val values = filters.map(keySpace.getIndexValues(_, ExplainNull))

  def compare(actual: Z2Filter, expected: Z2Filter): MatchResult[Boolean] = {
    val left = actual.xy.asInstanceOf[Array[AnyRef]]
    val right = expected.xy.asInstanceOf[Array[AnyRef]]
    java.util.Arrays.deepEquals(left, right) must beTrue
  }

  "Z2Filter" should {
    "serialize to and from bytes" in {
      forall(values) { value =>
        val filter = Z2Filter(value)
        val result = Z2Filter.deserializeFromBytes(Z2Filter.serializeToBytes(filter))
        compare(result, filter)
      }
    }
    "serialize to and from strings" in {
      forall(values) { value =>
        val filter = Z2Filter(value)
        val result = Z2Filter.deserializeFromStrings(Z2Filter.serializeToStrings(filter))
        compare(result, filter)
      }
    }
  }
}
