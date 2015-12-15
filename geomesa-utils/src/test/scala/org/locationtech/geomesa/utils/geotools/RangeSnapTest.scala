/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.utils.geotools

import com.typesafe.scalalogging.slf4j.Logging
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RangeSnapTest extends Specification with Logging {

  "RangeSnap" should {
    val buckets = 10
    val lowerEndpoint: java.lang.Long = -25L
    val upperEndpoint: java.lang.Long = 25L
    val interval: com.google.common.collect.Range[java.lang.Long] = com.google.common.collect.Ranges.closed(lowerEndpoint, upperEndpoint)
    val rangeSnap = new RangeSnap(interval, buckets)

    "create a snap around a given interval" in {
      rangeSnap must not beNull
    }

    "compute correct bin given attribute value outside interval" in {
      rangeSnap.getBucket(-100) must beEqualTo(-25)
      rangeSnap.getBucket(-50) must beEqualTo(-25)
      rangeSnap.getBucket(-26) must beEqualTo(-25)
      rangeSnap.getBucket(100) must beEqualTo(25)
      rangeSnap.getBucket(50) must beEqualTo(25)
      rangeSnap.getBucket(26) must beEqualTo(25)
    }

    "compute correct bin given attribute value inside interval" in {
      rangeSnap.getBucket(-25) must beEqualTo(-25)
      rangeSnap.getBucket(-24) must beEqualTo(-25)
      rangeSnap.getBucket(-23) must beEqualTo(-25)
      rangeSnap.getBucket(-22) must beEqualTo(-25)
      rangeSnap.getBucket(-21) must beEqualTo(-25)
      rangeSnap.getBucket(-20) must beEqualTo(-20)
      rangeSnap.getBucket(0) must beEqualTo(0)
      rangeSnap.getBucket(20) must beEqualTo(20)
      rangeSnap.getBucket(21) must beEqualTo(20)
      rangeSnap.getBucket(22) must beEqualTo(20)
      rangeSnap.getBucket(23) must beEqualTo(20)
      rangeSnap.getBucket(24) must beEqualTo(20)
      rangeSnap.getBucket(25) must beEqualTo(25)
    }
  }
}
