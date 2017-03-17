/*******************************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ******************************************************************************/

package org.locationtech.geomesa.arrow.vector

import org.apache.arrow.memory.RootAllocator
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SimpleFeatureVectorTest extends Specification {

  "SimpleFeatureVector" should {
    "read and write values" >> {
      val sft = SimpleFeatureTypes.createType("test", "name:String,dtg:Date,*geom:Point:srid=4326")
      val features = (0 until 10).map { i =>
        ScalaSimpleFeature.create(sft, s"0$i", s"name0$i", s"2017-03-15T00:0$i:00.000Z", s"POINT (4$i 5$i)")
      }
      val allocator = new RootAllocator(Long.MaxValue)
      try {
        val vector = new SimpleFeatureVector(sft, allocator)
        try {
          features.zipWithIndex.foreach { case (f, i) => vector.writer.write(i, f) }
          vector.writer.setValueCount(features.length)
          vector.reader.getValueCount mustEqual features.length
          forall(0 until 10) { i =>
            vector.reader.read(i) mustEqual features(i)
          }
        } finally {
          vector.close()
        }
      } finally {
        allocator.close()
      }
    }
  }
}
