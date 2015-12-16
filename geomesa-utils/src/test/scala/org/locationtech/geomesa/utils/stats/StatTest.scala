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
  val sft = SimpleFeatureTypes.createType("testType", "foo:Int,geom:Point,bar:Long")
  val fooIndex = sft.indexOf("foo")
  val barIndex = sft.indexOf("bar")

  "DSL" should {
    "create MinMax stat gather" in {
      val stats = Stat(sft, "MinMax(foo)")
      val stat  = stats.asInstanceOf[SeqStat].stats.head

      val mm = stat.asInstanceOf[MinMax[java.lang.Long]]
      mm.attributeIndex mustEqual fooIndex
    }

    "create a Sequence of Stats" in {
      val stat = Stat(sft, "MinMax(foo),MinMax(bar),IteratorCount")

      val stats = stat.asInstanceOf[SeqStat].stats

      stats.size mustEqual 3

      stats(0).asInstanceOf[MinMax[java.lang.Long]].attributeIndex mustEqual fooIndex
      stats(1).asInstanceOf[MinMax[java.lang.Long]].attributeIndex mustEqual barIndex

      stats(2) must beAnInstanceOf[IteratorStackCounter]
    }
  }
}
