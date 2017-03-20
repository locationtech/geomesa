/*******************************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ******************************************************************************/

package org.locationtech.geomesa.arrow.io

import java.io.{FileInputStream, FileOutputStream}
import java.nio.file.Files

import org.apache.arrow.memory.RootAllocator
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SimpleFeatureArrowFileTest extends Specification {

  "SimpleFeatureArrowFiles" should {
    "write and read values" >> {
      val sft = SimpleFeatureTypes.createType("test", "name:String,dtg:Date,*geom:Point:srid=4326")
      val features0 = (0 until 10).map { i =>
        ScalaSimpleFeature.create(sft, s"0$i", s"name0$i", s"2017-03-15T00:0$i:00.000Z", s"POINT (4$i 5$i)")
      }
      val features1 = (10 until 20).map { i =>
        ScalaSimpleFeature.create(sft, s"$i", s"name$i", s"2017-03-15T00:$i:00.000Z", s"POINT (4${i -10} 5${i -10})")
      }
      val file = Files.createTempFile("gm-arrow", "io").toFile
      val allocator = new RootAllocator(Long.MaxValue)
      try {
        val writer = new SimpleFeatureArrowFileWriter(sft, new FileOutputStream(file), allocator)
        try {
          features0.foreach(writer.add)
          writer.flush()
          features1.foreach(writer.add)
        } finally {
          writer.close()
        }

        val reader = new SimpleFeatureArrowFileReader(new FileInputStream(file), allocator)
        try {
          val features = reader.read().toSeq
          features must haveLength(20)
          features must containTheSameElementsAs(features0 ++ features1)
        } finally {
          reader.close()
        }
      } finally {
        if (!file.delete()) {
          file.deleteOnExit()
        }
//        allocator.close()
      }
    }
  }
}
