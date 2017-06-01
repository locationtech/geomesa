/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.util

import com.typesafe.scalalogging.LazyLogging
import org.junit.{Assert, Test}
import org.locationtech.geomesa.utils.geohash.{BoundingBox, GeoHash}

class BoundingBoxUtilTest extends LazyLogging {
  @Test def getRangesTest {
    val bbox: BoundingBox = BoundingBox.apply(GeoHash.apply("tmzcrzpt").getPoint, GeoHash.apply("ttb12p21").getPoint)
    val ranges = BoundingBoxUtil.getRanges(bbox)
    logger.debug("" + ranges)
    Assert.assertEquals(15, ranges.size)
  }
}