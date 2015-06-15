/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/


package org.locationtech.geomesa.accumulo.iterators

import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DeDuplicatingIteratorTest extends Specification {

  "DeDuplicatingIterator" should {
    "filter on unique elements" in {
      val sft = SimpleFeatureTypes.createType("test", "*geom:Point:srid=4326")
      val attributes = Array[AnyRef]("POINT(0,0)")
      val features = Seq(
        new ScalaSimpleFeature("1", sft, attributes),
        new ScalaSimpleFeature("2", sft, attributes),
        new ScalaSimpleFeature("0", sft, attributes),
        new ScalaSimpleFeature("1", sft, attributes),
        new ScalaSimpleFeature("3", sft, attributes),
        new ScalaSimpleFeature("4", sft, attributes),
        new ScalaSimpleFeature("1", sft, attributes),
        new ScalaSimpleFeature("3", sft, attributes)
      )
      val deduped = new DeDuplicatingIterator(features.toIterator).toSeq
      deduped must haveLength(5)
      deduped.map(_.getID) mustEqual Seq("1", "2", "0", "3", "4")
    }
  }
}