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
import org.locationtech.geomesa.index.index.z3.Z3IndexKeySpace
import org.locationtech.geomesa.index.utils.ExplainNull
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class Z3FilterTest extends Specification {

  val sft = SimpleFeatureTypes.createType("z3FilterTest", "dtg:Date,*geom:Point:srid=4326")

  val keySpace = new Z3IndexKeySpace(sft, NoShardStrategy, "geom", "dtg")

  val filters = Seq(
    "bbox(geom,38,48,52,62) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z",
    "bbox(geom,38,48,52,62) and dtg DURING 2013-12-15T00:00:00.000Z/2014-01-15T00:00:00.000Z",
    "dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z"
  ).map(ECQL.toFilter)

  val values = filters.map(keySpace.getIndexValues(_, ExplainNull))

  def compare(actual: Z3Filter, expected: Z3Filter): MatchResult[Boolean] = {
    val left = Array[AnyRef](actual.xy, actual.t, Short.box(actual.minEpoch), Short.box(actual.maxEpoch))
    val right = Array[AnyRef](expected.xy, expected.t, Short.box(expected.minEpoch), Short.box(expected.maxEpoch))
    java.util.Arrays.deepEquals(left, right) must beTrue
  }

  "Z3Filter" should {
    "serialize to and from bytes" in {
      forall(values) { value =>
        val filter = Z3Filter(value)
        val result = Z3Filter.deserializeFromBytes(Z3Filter.serializeToBytes(filter))
        compare(result, filter)
      }
    }
    "serialize to and from strings" in {
      forall(values) { value =>
        val filter = Z3Filter(value)
        val result = Z3Filter.deserializeFromStrings(Z3Filter.serializeToStrings(filter))
        compare(result, filter)
      }
    }
  }
}
