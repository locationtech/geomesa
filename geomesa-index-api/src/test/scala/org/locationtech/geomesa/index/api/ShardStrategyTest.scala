/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.api

import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.conf.ColumnGroups
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ShardStrategyTest extends Specification {
  "ShardStrategy" should {
    "handle negative hash values" in {
      val sft = SimpleFeatureTypes.createType("hash", "geom:Point,dtg:Date;geomesa.z.splits=60")
      val wrapper = WritableFeature.wrapper(sft, new ColumnGroups)
      val sf = ScalaSimpleFeature.create(sft, "1371494157#3638946185",
        "POINT (88.3176015 22.5988557)", "2019-12-23T01:00:00.000Z")
      val writable = wrapper.wrap(sf)
      val strategy = ShardStrategy(60)
      strategy.apply(writable) must not(beNull)
    }
  }
}
