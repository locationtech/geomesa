/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export

import org.junit.runner.RunWith
import org.locationtech.geomesa.tools.export.ExportCommand.FileSizeEstimator
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FileSizeEstimatorTest extends Specification {

  "FileSizeEstimator" should {
    "estimate feature count for a given file size" in {
      val estimator = new FileSizeEstimator(1000, 0.1f, 100)
      estimator.estimate(0) mustEqual 10
      estimator.estimate(100) mustEqual 9
      estimator.estimate(500) mustEqual 5
    }
    "update estimates if too low" in {
      val estimator = new FileSizeEstimator(1000, 0.1f, 100)
      estimator.estimate(0) mustEqual 10
      estimator.update(1101, 10)
      estimator.estimate(0) mustEqual 9
    }
    "update estimates if too high" in {
      val estimator = new FileSizeEstimator(1000, 0.1f, 100)
      estimator.estimate(0) mustEqual 10
      estimator.update(899, 10)
      estimator.estimate(0) mustEqual 11
    }
    "not update estimate if within error threshold" in {
      val estimator = new FileSizeEstimator(1000, 0.1f, 100)
      estimator.estimate(0) mustEqual 10
      estimator.update(1100, 10)
      estimator.estimate(0) mustEqual 10
    }
    "always estimate at least 1" in {
      val estimator = new FileSizeEstimator(1000, 0.1f, 100)
      forall(Range(0, 2000, 100))(i => estimator.estimate(i) must beGreaterThanOrEqualTo(1))
    }
  }
}
