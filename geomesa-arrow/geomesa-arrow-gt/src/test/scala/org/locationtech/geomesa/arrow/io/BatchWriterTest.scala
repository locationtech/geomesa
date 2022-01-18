/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.io

import java.io.ByteArrayInputStream
import java.util.Date

import org.apache.arrow.vector.ipc.message.IpcOption
import org.junit.runner.RunWith
import org.locationtech.geomesa.arrow.ArrowAllocator
import org.locationtech.geomesa.arrow.io.records.RecordBatchUnloader
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.arrow.vector.{ArrowDictionary, SimpleFeatureVector}
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class BatchWriterTest extends Specification {
  sequential

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

  "BatchWriter" should {
    "merge sort arrow batches" >> {
      val encoding = SimpleFeatureEncoding.min(includeFids = true)
      val dictionaries = Map.empty[String, ArrowDictionary]
      val batches: Seq[Array[Byte]] = buildBatches(encoding, dictionaries)

      val bytes = WithClose(BatchWriter.reduce(sft, dictionaries, encoding, new IpcOption(), Some("dtg" -> false), sorted = false, 10, batches.iterator))(_.reduceLeft(_ ++ _))

      val features = WithClose(SimpleFeatureArrowFileReader.streaming(() => new ByteArrayInputStream(bytes))) { reader =>
        WithClose(reader.features())(_.map(ScalaSimpleFeature.copy).toList)
      }

      features must haveLength(30)
      // note: didn't encode feature id
      features.map(_.getAttributes) mustEqual
          (features0 ++ features1 ++ features2).sortBy(_.getAttribute("dtg").asInstanceOf[Date]).map(_.getAttributes)
    }

    "not leak memory when iteration dies" >> {
      val encoding = SimpleFeatureEncoding.min(includeFids = true)
      val dictionaries = Map.empty[String, ArrowDictionary]
      val batches: Seq[Array[Byte]] = buildBatches(encoding, dictionaries)

      val iterator: Iterator[Array[Byte]] = new Iterator[Array[Byte]] {
        private val internal = batches.iterator
        override def hasNext: Boolean = {
          if (internal.hasNext) {
            true
          } else {
            throw new Exception("No more elements!")
          }
        }

        override def next(): Array[Byte] = {
          internal.next()
        }
      }

      try {
        WithClose(BatchWriter.reduce(sft, dictionaries, encoding, new IpcOption(), Some("dtg" -> false), sorted = false, 10, iterator))(_.reduceLeft(_ ++ _))
      } catch {
        case _: Exception =>
          // The iterator passed in throws an exception.  That stops the BatchWriter.
          // The goal of this test is to show that off-heap memory is not leaked when that happens.
      }
      ArrowAllocator.getAllocatedMemory("test") mustEqual 0
    }
  }

  private def buildBatches(encoding: SimpleFeatureEncoding, dictionaries: Map[String, ArrowDictionary]) = {
    val batches = WithClose(SimpleFeatureVector.create(sft, dictionaries, encoding)) { vector =>
      val unloader = new RecordBatchUnloader(vector, new IpcOption())
      Seq(features0, features1, features2).map { features =>
        var i = 0
        while (i < features.length) {
          vector.writer.set(i, features(i))
          i += 1
        }
        unloader.unload(i)
      }
    }
    batches
  }
}
