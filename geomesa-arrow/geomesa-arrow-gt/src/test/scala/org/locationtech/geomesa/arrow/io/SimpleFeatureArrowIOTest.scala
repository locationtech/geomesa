/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.io

import java.util.Date

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.DirtyRootAllocator
import org.apache.arrow.vector.complex.StructVector
import org.junit.runner.RunWith
import org.locationtech.geomesa.arrow.io.records.{RecordBatchLoader, RecordBatchUnloader}
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.arrow.vector.{ArrowDictionary, SimpleFeatureVector}
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SimpleFeatureArrowIOTest extends Specification {

  implicit val allocator: BufferAllocator = new DirtyRootAllocator(Long.MaxValue, 6.toByte)

  val sft = SimpleFeatureTypes.createType("test", "name:String,foo:String,dtg:Date,*geom:Point:srid=4326")

  val features0 = (0 until 10).map { i =>
    ScalaSimpleFeature.create(sft, s"0$i", s"name0$i", s"foo${i % 2}", s"2017-03-15T00:0$i:00.000Z", s"POINT (4$i 5$i)")
  }
  val features1 = (10 until 20).map { i =>
    ScalaSimpleFeature.create(sft, s"$i", s"name$i", s"foo${i % 3}", s"2017-03-15T00:0${i-10}:20.000Z", s"POINT (4${i -10} 5${i -10})")
  }
  val features2 = (20 until 30).map { i =>
    ScalaSimpleFeature.create(sft, s"$i", s"name$i", s"foo${i % 3}", s"2017-03-15T00:0${i-20}:10.000Z", s"POINT (4${i -20} 5${i -20})")
  }

  "SimpleFeatureArrowIO" should {
    "merge sort arrow batches" >> {
      val encoding = SimpleFeatureEncoding.min(includeFids = true)
      val dictionaries = Map.empty[String, ArrowDictionary]
      val (field, batches) = WithClose(SimpleFeatureVector.create(sft, dictionaries, encoding)) { vector =>
        val unloader = new RecordBatchUnloader(vector)
        val batches = Seq(features0, features1, features2).map { features =>
          var i = 0
          while (i < features.length) {
            vector.writer.set(i, features(i))
            i += 1
          }
          unloader.unload(i)
        }
        (vector.underlying.getField, batches)
      }

      val features = WithClose(SimpleFeatureArrowIO.sortBatches(sft, dictionaries, encoding, "dtg", reverse = false, 10, batches.iterator)) { sorted =>
        val loader = RecordBatchLoader(field)
        WithClose(SimpleFeatureVector.wrap(loader.vector.asInstanceOf[StructVector], dictionaries)) { vector =>
          sorted.flatMap { batch =>
            vector.clear()
            loader.load(batch)
            (0 until vector.reader.getValueCount).map(i => ScalaSimpleFeature.copy(vector.reader.get(i)))
          }.toList
        }
      }

      features must haveLength(30)
      // note: didn't encode feature id
      features.map(_.getAttributes) mustEqual
          (features0 ++ features1 ++ features2).sortBy(_.getAttribute("dtg").asInstanceOf[Date]).map(_.getAttributes)
    }
  }

  step {
    allocator.close()
  }
}
