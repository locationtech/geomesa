/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.vector

import java.util.Date

import org.apache.arrow.memory.RootAllocator
import org.geotools.util.Converters
import org.junit.runner.RunWith
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SimpleFeatureVectorTest extends Specification {

  val sft = SimpleFeatureTypes.createType("test", "name:String,dtg:Date,*geom:Point:srid=4326")
  val features = (0 until 10).map { i =>
    ScalaSimpleFeature.create(sft, s"0$i", s"name0${i % 2}", s"2017-03-15T00:0$i:00.000Z", s"POINT (4$i 5$i)")
  }

  implicit val allocator = new RootAllocator(Long.MaxValue)

  "SimpleFeatureVector" should {
    "set and get values" >> {
      WithClose(SimpleFeatureVector.create(sft, Map.empty, SimpleFeatureEncoding.max(true))) { vector =>
        features.zipWithIndex.foreach { case (f, i) => vector.writer.set(i, f) }
        vector.writer.setValueCount(features.length)
        vector.reader.getValueCount mustEqual features.length
        forall(0 until 10)(i => vector.reader.get(i) mustEqual features(i))
        // check wrapping
        WithClose(SimpleFeatureVector.wrap(vector.underlying, Map.empty)) { wrapped =>
          wrapped.reader.getValueCount mustEqual features.length
          forall(0 until 10)(i => wrapped.reader.get(i) mustEqual features(i))
        }
      }
    }
    "expand capacity" >> {
      val total = 128
      WithClose(SimpleFeatureVector.create(sft, Map.empty, SimpleFeatureEncoding.max(true), capacity = total / 2)) { vector =>
        var i = 0
        while (i < total) {
          vector.writer.set(i, features(i % features.length))
          i += 1
        }
        vector.writer.setValueCount(total)
        vector.reader.getValueCount mustEqual total
        forall(0 until total)(i => vector.reader.get(i) mustEqual features(i % features.length))
      }
    }
    "set and get float precision values" >> {
      WithClose(SimpleFeatureVector.create(sft, Map.empty, SimpleFeatureEncoding.min(true))) { vector =>
        features.zipWithIndex.foreach { case (f, i) => vector.writer.set(i, f) }
        vector.writer.setValueCount(features.length)
        vector.reader.getValueCount mustEqual features.length
        forall(0 until 10)(i => vector.reader.get(i) mustEqual features(i))
        // check wrapping
        WithClose(SimpleFeatureVector.wrap(vector.underlying, Map.empty)) { wrapped =>
          wrapped.reader.getValueCount mustEqual features.length
          forall(0 until 10)(i => wrapped.reader.get(i) mustEqual features(i))
        }
      }
    }
    "exclude feature ids" >> {
      WithClose(SimpleFeatureVector.create(sft, Map.empty, SimpleFeatureEncoding.min(false))) { vector =>
        features.zipWithIndex.foreach { case (f, i) => vector.writer.set(i, f) }
        vector.writer.setValueCount(features.length)
        vector.reader.getValueCount mustEqual features.length
        forall(0 until 10) { i =>
          val read = vector.reader.get(i)
          read.getAttributes mustEqual features(i).getAttributes
          read.getID mustNotEqual features(i).getID
        }
        // check wrapping
        WithClose(SimpleFeatureVector.wrap(vector.underlying, Map.empty)) { wrapped =>
          wrapped.reader.getValueCount mustEqual features.length
          forall(0 until 10) { i =>
            val read = vector.reader.get(i)
            read.getAttributes mustEqual features(i).getAttributes
            read.getID mustNotEqual features(i).getID
          }
        }
      }
    }
    "set and get dictionary encoded values" >> {
      val dictionary = Map("name" -> ArrowDictionary.create(Seq("name00", "name01")))
      WithClose(SimpleFeatureVector.create(sft, dictionary, SimpleFeatureEncoding.max(true))) { vector =>
        features.zipWithIndex.foreach { case (f, i) => vector.writer.set(i, f) }
        vector.writer.setValueCount(features.length)
        vector.reader.getValueCount mustEqual features.length
        forall(0 until 10)(i => vector.reader.get(i) mustEqual features(i))
        // check wrapping
        WithClose(SimpleFeatureVector.wrap(vector.underlying, dictionary)) { wrapped =>
          wrapped.reader.getValueCount mustEqual features.length
          forall(0 until 10)(i => wrapped.reader.get(i) mustEqual features(i))
        }
      }
    }
    "set and get lists and maps" >> {
      import scala.collection.JavaConverters._
      val sft = SimpleFeatureTypes.createType("test",
        "name:String,tags:Map[String,String],dates:List[Date],*geom:Point:srid=4326")
      val features = (0 until 10).map { i =>
        val dates = Seq(s"2017-03-15T00:0$i:00.000Z", s"2017-03-15T00:0$i:10.000Z", s"2017-03-15T00:0$i:20.000Z")
            .map(Converters.convert(_, classOf[Date])).asJava
        val tags = Map(s"a$i" -> s"av$i", s"b$i" -> s"bv$i").asJava
        ScalaSimpleFeature.create(sft, s"0$i", s"name0${i % 2}", tags, dates, s"POINT (4$i 5$i)")
      }
      WithClose(SimpleFeatureVector.create(sft, Map.empty, SimpleFeatureEncoding.max(true))) { vector =>
        features.zipWithIndex.foreach { case (f, i) => vector.writer.set(i, f) }
        vector.writer.setValueCount(features.length)
        vector.reader.getValueCount mustEqual features.length
        forall(0 until 10)(i => vector.reader.get(i) mustEqual features(i))
        // check wrapping
        WithClose(SimpleFeatureVector.wrap(vector.underlying, Map.empty)) { wrapped =>
          wrapped.reader.getValueCount mustEqual features.length
          forall(0 until 10)(i => wrapped.reader.get(i) mustEqual features(i))
        }
      }
    }
  }

  step {
    allocator.close()
  }
}
