/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.vector

import java.util.Date

import org.apache.arrow.vector.complex.FixedSizeListVector
import org.apache.arrow.vector.{BigIntVector, IntVector}
import org.geotools.util.Converters
import org.junit.runner.RunWith
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.filter.function.ProxyIdFunction
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SimpleFeatureVectorTest extends Specification {

  val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
  val features = (0 until 10).map { i =>
    ScalaSimpleFeature.create(sft, s"28a12c18-e5ae-4c04-ae7b-bf7cdbfaf23$i", s"name0${i % 2}",
      s"${i % 5}", s"2017-03-15T00:0$i:00.000Z", s"POINT (4$i 5$i)")
  }

  val uuidSft = SimpleFeatureTypes.createType(sft.getTypeName,
    SimpleFeatureTypes.encodeType(sft) + s";${Configs.FidsAreUuids}=true")

  "SimpleFeatureVector" should {
    "set and get values" >> {
      WithClose(SimpleFeatureVector.create(sft, Map.empty, SimpleFeatureEncoding.Max)) { vector =>
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
      WithClose(SimpleFeatureVector.create(sft, Map.empty, SimpleFeatureEncoding.Max, capacity = total / 2)) { vector =>
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
      WithClose(SimpleFeatureVector.create(sft, Map.empty, SimpleFeatureEncoding.min(includeFids = true))) { vector =>
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
    "set and get feature uuids" >> {
      WithClose(SimpleFeatureVector.create(uuidSft, Map.empty, SimpleFeatureEncoding.Max)) { vector =>
        features.zipWithIndex.foreach { case (f, i) => vector.writer.set(i, f) }
        vector.writer.setValueCount(features.length)
        vector.reader.getValueCount mustEqual features.length
        forall(0 until 10)(i => vector.reader.get(i) mustEqual features(i))
        // verify that id is encoded as 2 longs
        val idVector = vector.underlying.getChild(SimpleFeatureVector.FeatureIdField)
        idVector must beAnInstanceOf[FixedSizeListVector]
        idVector.asInstanceOf[FixedSizeListVector].getDataVector must beAnInstanceOf[BigIntVector]
        // check wrapping
        WithClose(SimpleFeatureVector.wrap(vector.underlying, Map.empty)) { wrapped =>
          wrapped.reader.getValueCount mustEqual features.length
          forall(0 until 10)(i => wrapped.reader.get(i) mustEqual features(i))
        }
      }
    }
    "proxy feature ids" >> {
      val encoding = SimpleFeatureEncoding.min(includeFids = true, proxyFids = true)
      val proxy = new ProxyIdFunction()
      foreach(Seq(sft, uuidSft)) { sft =>
        WithClose(SimpleFeatureVector.create(sft, Map.empty, encoding)) { vector =>
          features.zipWithIndex.foreach { case (f, i) => vector.writer.set(i, f) }
          vector.writer.setValueCount(features.length)
          vector.reader.getValueCount mustEqual features.length
          forall(0 until 10) { i =>
            val read = vector.reader.get(i)
            read.getAttributes mustEqual features(i).getAttributes
            // note: copy the test feature so that the uuid hint is in the feature sft
            read.getID.toInt mustEqual proxy.evaluate(ScalaSimpleFeature.copy(sft, features(i)))
          }
          // verify that id is encoded as an int
          val idVector = vector.underlying.getChild(SimpleFeatureVector.FeatureIdField)
          idVector must beAnInstanceOf[IntVector]
          // check wrapping
          WithClose(SimpleFeatureVector.wrap(vector.underlying, Map.empty)) { wrapped =>
            wrapped.reader.getValueCount mustEqual features.length
            forall(0 until 10) { i =>
              val read = vector.reader.get(i)
              read.getAttributes mustEqual features(i).getAttributes
              // note: copy the test feature so that the uuid hint is in the feature sft
              read.getID.toInt mustEqual proxy.evaluate(ScalaSimpleFeature.copy(sft, features(i)))
            }
          }
        }
      }
    }
    "set and get null values" >> {
      WithClose(SimpleFeatureVector.create(sft, Map.empty, SimpleFeatureEncoding.min(includeFids = true))) { vector =>
        val nulls = features.map(ScalaSimpleFeature.copy)
        (0 until sft.getAttributeCount).foreach(i => nulls.foreach(_.setAttribute(i, null)))
        nulls.zipWithIndex.foreach { case (f, i) => vector.writer.set(i, f) }
        vector.writer.setValueCount(features.length)
        vector.reader.getValueCount mustEqual features.length
        forall(0 until 10)(i => vector.reader.get(i) mustEqual nulls(i))
        // check wrapping
        WithClose(SimpleFeatureVector.wrap(vector.underlying, Map.empty)) { wrapped =>
          wrapped.reader.getValueCount mustEqual features.length
          forall(0 until 10)(i => wrapped.reader.get(i) mustEqual nulls(i))
        }
      }
    }
    "exclude feature ids" >> {
      WithClose(SimpleFeatureVector.create(sft, Map.empty, SimpleFeatureEncoding.min(includeFids = false))) { vector =>
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
      val dictionary = Map("name" -> ArrowDictionary.create(sft.getTypeName, 0, Array("name00", "name01")))
      WithClose(SimpleFeatureVector.create(sft, dictionary, SimpleFeatureEncoding.Max)) { vector =>
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
    "set and get null dictionary values" >> {
      val dictionary = Map("name" -> ArrowDictionary.create(sft.getTypeName, 0, Array("name00", "name01", null)))
      WithClose(SimpleFeatureVector.create(sft, dictionary, SimpleFeatureEncoding.min(includeFids = true))) { vector =>
        val nulls = features.map(ScalaSimpleFeature.copy)
        (0 until sft.getAttributeCount).foreach(i => nulls.foreach(_.setAttribute(i, null)))
        nulls.zipWithIndex.foreach { case (f, i) => vector.writer.set(i, f) }
        vector.writer.setValueCount(features.length)
        vector.reader.getValueCount mustEqual features.length
        forall(0 until 10)(i => vector.reader.get(i) mustEqual nulls(i))
        // check wrapping
        WithClose(SimpleFeatureVector.wrap(vector.underlying, dictionary)) { wrapped =>
          wrapped.reader.getValueCount mustEqual features.length
          forall(0 until 10)(i => wrapped.reader.get(i) mustEqual nulls(i))
        }
      }
    }
    "set and get dictionary encoded ints" >> {
      val dictionary = Map("age" -> ArrowDictionary.create(sft.getTypeName, 0, Array(0, 1, 2, 3, 4, 5).map(Int.box)))
      WithClose(SimpleFeatureVector.create(sft, dictionary, SimpleFeatureEncoding.Max)) { vector =>
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
      WithClose(SimpleFeatureVector.create(sft, Map.empty, SimpleFeatureEncoding.Max)) { vector =>
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
    "set and get dictionary encoded lists" >> {
      import scala.collection.JavaConverters._
      val sft = SimpleFeatureTypes.createType("test", "names:List[String],dtg:Date,*geom:Point:srid=4326")
      val features = (0 until 10).map { i =>
        val names = Seq.tabulate(i)(j => s"name0${j % 5}").asJava
        ScalaSimpleFeature.create(sft, s"0$i", names, s"2017-03-15T00:0$i:00.000Z", s"POINT (4$i 5$i)")
      }
      val dictionary = Map("names" -> ArrowDictionary.create(sft.getTypeName, 0, Array("name00", "name01", "name02", "name03", "name04")))
      WithClose(SimpleFeatureVector.create(sft, dictionary, SimpleFeatureEncoding.min(includeFids = true))) { vector =>
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
    "set null geometries" >> {
      val sft = SimpleFeatureTypes.createType("test",
        "line:LineString:srid=4326,poly:Polygon:srid=4326,*geom:Point:srid=4326")
      val features = (0 until 10).map { i =>
        ScalaSimpleFeature.create(sft, s"$i", s"LINESTRING (30 10, 1$i 30, 40 40)",
          s"POLYGON ((30 10, 4$i 40, 20 40, 10 20, 30 10))", s"POINT (4$i 5$i)")
      }
      WithClose(SimpleFeatureVector.create(sft, Map.empty, SimpleFeatureEncoding.Max)) { vector =>
        features.zipWithIndex.foreach { case (f, i) => vector.writer.set(i, f) }
        vector.writer.setValueCount(features.length)
        vector.reader.getValueCount mustEqual features.length
        forall(0 until 10)(i => vector.reader.get(i) mustEqual features(i))

        vector.clear()

        val nulls = features.map(ScalaSimpleFeature.copy)
        (0 until sft.getAttributeCount).foreach(i => nulls.foreach(_.setAttribute(i, null)))
        nulls.zipWithIndex.foreach { case (f, i) => vector.writer.set(i, f) }
        vector.writer.setValueCount(features.length)
        vector.reader.getValueCount mustEqual features.length
        forall(0 until 10)(i => vector.reader.get(i) mustEqual nulls(i))
      }
    }
    "set and get line strings" >> {
      val sft = SimpleFeatureTypes.createType("test", "name:String,*geom:LineString:srid=4326")
      val features = (0 until 10).map { i =>
        ScalaSimpleFeature.create(sft, s"0$i", s"name0${i % 2}", s"LINESTRING (30 10, 1$i 30, 40 40)")
      }
      WithClose(SimpleFeatureVector.create(sft, Map.empty, SimpleFeatureEncoding.min(includeFids = true))) { vector =>
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
    "clear" >> {
      WithClose(SimpleFeatureVector.create(sft, Map.empty, SimpleFeatureEncoding.min(includeFids = true))) { vector =>
        features.zipWithIndex.foreach { case (f, i) => vector.writer.set(i, f) }
        vector.writer.setValueCount(features.length)
        vector.reader.getValueCount mustEqual features.length
        forall(0 until 10)(i => vector.reader.get(i) mustEqual features(i))

        vector.clear()

        val nulls = features.map(ScalaSimpleFeature.copy)
        (0 until sft.getAttributeCount).foreach(i => nulls.foreach(_.setAttribute(i, null)))
        nulls.zipWithIndex.foreach { case (f, i) => vector.writer.set(i, f) }
        vector.writer.setValueCount(features.length)
        vector.reader.getValueCount mustEqual features.length
        forall(0 until 10)(i => vector.reader.get(i) mustEqual nulls(i))
      }
    }
  }
}
