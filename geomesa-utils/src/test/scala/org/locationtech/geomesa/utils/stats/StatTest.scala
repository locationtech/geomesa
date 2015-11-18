/***********************************************************************
  * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0 which
  * accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  *************************************************************************/

package org.locationtech.geomesa.utils.stats

import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class StatTest extends Specification  {

  // Stat's apply method shoul take a SFT and do light validation.
  //val sft = SimpleFeatureTypes.createType()

  "DSL" should {
    "create MinMax stat gather" in {
      val stats = Stat("MinMax(foo)")
      val stat  = stats.asInstanceOf[SeqStat].stats.head

      val mm = stat.asInstanceOf[MinMax[java.lang.Long]]
      mm.attribute mustEqual "foo"
    }

    "create a Sequence of Stats" in {
      val stat = Stat("MinMax(foo),MinMax(bar),IteratorCount")

      val stats = stat.asInstanceOf[SeqStat].stats

      stats.size mustEqual 3

      stats(0).asInstanceOf[MinMax[java.lang.Long]].attribute mustEqual "foo"
      stats(1).asInstanceOf[MinMax[java.lang.Long]].attribute mustEqual "bar"

      stats(2) must beAnInstanceOf[IteratorStackCounter]
    }
  }
}
