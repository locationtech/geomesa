/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geohash

import com.typesafe.scalalogging.LazyLogging
import org.junit.{Assert, Test}

class BoundingBoxTest extends LazyLogging {
  @Test def boundingBoxTest {
    var bbox = BoundingBox.apply(GeoHash.apply("dqb00").getPoint, GeoHash.apply("dqbxx").getPoint)
    var hashes = BoundingBox.getGeoHashesFromBoundingBox(bbox)
    logger.debug(hashes.size + "\n" + hashes)
    Assert.assertEquals(24, hashes.size)

    bbox = BoundingBox.apply(-78, -77.895029, 38.045834, 38)
    hashes = BoundingBox.getGeoHashesFromBoundingBox(bbox, 32)
    logger.debug(hashes.size + "\n" + hashes)
    Assert.assertEquals(6, hashes.size)

    bbox = BoundingBox.apply(-78, -77.89503, 38.0458335, 38)
    hashes = BoundingBox.getGeoHashesFromBoundingBox(bbox, 32)
    logger.debug(hashes.size + "\n" + hashes)
    Assert.assertEquals(6, hashes.size)

    bbox = BoundingBox.apply(-50, 50, -40, 40)
    hashes = BoundingBox.getGeoHashesFromBoundingBox(bbox, 32)
    logger.debug(hashes.size + "\n" + hashes)
    Assert.assertEquals(8, hashes.size)

    bbox = BoundingBox.apply(1, 1, 1, 1)
    hashes = BoundingBox.getGeoHashesFromBoundingBox(bbox, 32)
    logger.debug(hashes.size + "\n" + hashes)
    Assert.assertEquals(1, hashes.size)

  }
}
