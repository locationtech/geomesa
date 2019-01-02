/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.util

import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geohash.{BoundingBox, GeoHash}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class BoundingBoxUtilTest extends Specification {
  "BoundingBoxUtil" should {
    "get ranges" >> {
      val bbox: BoundingBox = BoundingBox.apply(GeoHash.apply("tmzcrzpt").getPoint, GeoHash.apply("ttb12p21").getPoint)
      val ranges = BoundingBoxUtil.getRanges(bbox)
      ranges must haveLength(15)
    }
  }
}
