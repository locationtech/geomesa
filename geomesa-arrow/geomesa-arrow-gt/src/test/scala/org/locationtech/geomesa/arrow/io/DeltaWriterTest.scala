/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.io

import java.io.ByteArrayInputStream

import org.locationtech.jts.geom.LineString
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.DirtyRootAllocator
import org.junit.runner.RunWith
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.mutable.ArrayBuffer

@RunWith(classOf[JUnitRunner])
class DeltaWriterTest extends Specification {

  val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
  val lineSft = SimpleFeatureTypes.createType("test", "name:String,team:String,age:Int,weight:Int,dtg:Date,*geom:LineString:srid=4326")

  val features = (0 until 20).map { i =>
    ScalaSimpleFeature.create(sft, s"0$i", s"name0${i % 2}", s"${i % 5}", f"2017-03-15T00:$i%02d:00.000Z", s"POINT (4${i % 10} 5${i % 10})")
  }
  val lineFeatures = (0 until 10).map { i =>
    val name = s"name${i % 2}"
    val team = s"team$i"
    val age = i % 5
    val weight = Option(i % 3).filter(_ != 0).map(Int.box).orNull
    val geom = s"LINESTRING(40 6$i, 40.1 6$i, 40.2 6$i, 40.3 6$i)"
    ScalaSimpleFeature.create(lineSft, s"$i", name, team, age, weight, s"2017-02-03T00:0$i:01.000Z", geom)
  }

  implicit val allocator: BufferAllocator = new DirtyRootAllocator(Long.MaxValue, 6.toByte)

  "DeltaWriter" should {
    "dynamically encode dictionary values without sorting" >> {
      val dictionaries = Seq("name", "age")
      val encoding = SimpleFeatureEncoding.min(includeFids = true)
      val result = ArrayBuffer.empty[Array[Byte]]

      WithClose(new DeltaWriter(sft, dictionaries, encoding, None, 10)) { writer =>
        result.append(writer.encode(features.drop(0).toArray, 3))
        result.append(writer.encode(features.drop(3).toArray, 5))
        result.append(writer.encode(features.drop(8).toArray, 2))
      }
      WithClose(new DeltaWriter(sft, dictionaries, encoding, None, 10)) { writer =>
        result.append(writer.encode(features.drop(15).toArray, 5))
        result.append(writer.encode(features.drop(10).toArray, 5))
      }

      val bytes = WithClose(DeltaWriter.reduce(sft, dictionaries, encoding, None, 5)(result.iterator)) { iter =>
        iter.foldLeft(Array.empty[Byte])(_ ++ _)
      }

      WithClose(SimpleFeatureArrowFileReader.streaming(() => new ByteArrayInputStream(bytes))) { reader =>
        reader.dictionaries must haveSize(2)
        reader.dictionaries.get("name") must beSome
        reader.dictionaries("name").iterator.toSeq must containTheSameElementsAs(Seq("name00", "name01"))
        reader.dictionaries.get("age") must beSome
        reader.dictionaries("age").iterator.toSeq must containTheSameElementsAs(0 until 5)

        WithClose(reader.features())(f => f.map(ScalaSimpleFeature.copy).toSeq must containTheSameElementsAs(features))
      }
    }
    "dynamically encode dictionary values with sorting" >> {
      val dictionaries = Seq("name", "age")
      val encoding = SimpleFeatureEncoding.min(includeFids = true)
      val sort = Some(("dtg", false))
      val result = ArrayBuffer.empty[Array[Byte]]

      WithClose(new DeltaWriter(sft, dictionaries, encoding, sort, 10)) { writer =>
        result.append(writer.encode(features.drop(0).toArray, 3))
        result.append(writer.encode(features.drop(3).toArray, 5))
        result.append(writer.encode(features.drop(8).toArray, 2))
      }
      WithClose(new DeltaWriter(sft, dictionaries, encoding, sort, 10)) { writer =>
        result.append(writer.encode(features.drop(15).toArray, 5))
        result.append(writer.encode(features.drop(10).toArray, 5))
      }

      val bytes = WithClose(DeltaWriter.reduce(sft, dictionaries, encoding, sort, 5)(result.iterator)) { iter =>
        iter.foldLeft(Array.empty[Byte])(_ ++ _)
      }

      WithClose(SimpleFeatureArrowFileReader.streaming(() => new ByteArrayInputStream(bytes))) { reader =>
        reader.dictionaries must haveSize(2)
        reader.dictionaries.get("name") must beSome
        reader.dictionaries("name").iterator.toSeq must containTheSameElementsAs(Seq("name00", "name01"))
        reader.dictionaries.get("age") must beSome
        reader.dictionaries("age").iterator.toSeq must containTheSameElementsAs(0 until 5)

        WithClose(reader.features())(f => f.map(ScalaSimpleFeature.copy).toVector mustEqual features)
      }
    }
    "work with line strings" >> {
      val encoding = SimpleFeatureEncoding.min(includeFids = true)
      val result = ArrayBuffer.empty[Array[Byte]]

      WithClose(new DeltaWriter(lineSft, Seq.empty, encoding, None, 10)) { writer =>
        result.append(writer.encode(lineFeatures.drop(0).toArray, 3))
        result.append(writer.encode(lineFeatures.drop(3).toArray, 5))
        result.append(writer.encode(lineFeatures.drop(8).toArray, 2))
      }

      val bytes = WithClose(DeltaWriter.reduce(lineSft, Seq.empty, encoding, None, 10)(result.iterator)) { iter =>
        iter.foldLeft(Array.empty[Byte])(_ ++ _)
      }

      WithClose(SimpleFeatureArrowFileReader.streaming(() => new ByteArrayInputStream(bytes))) { reader =>
        reader.dictionaries must beEmpty
        testLineReader(reader)
      }
    }
    "work with sorted line strings" >> {
      val encoding = SimpleFeatureEncoding.min(includeFids = true)
      val result = ArrayBuffer.empty[Array[Byte]]
      val sort = Some(("dtg", false))

      WithClose(new DeltaWriter(lineSft, Seq.empty, encoding, sort, 10)) { writer =>
        result.append(writer.encode(lineFeatures.drop(3).toArray, 5))
        result.append(writer.encode(lineFeatures.drop(0).toArray, 3))
        result.append(writer.encode(lineFeatures.drop(8).toArray, 2))
      }

      val bytes = WithClose(DeltaWriter.reduce(lineSft, Seq.empty, encoding, sort, 10)(result.iterator)) { iter =>
        iter.foldLeft(Array.empty[Byte])(_ ++ _)
      }

      WithClose(SimpleFeatureArrowFileReader.streaming(() => new ByteArrayInputStream(bytes))) { reader =>
        reader.dictionaries must beEmpty
        testLineReader(reader)
      }
    }
  }


  def testLineReader(reader: SimpleFeatureArrowFileReader): MatchResult[Any] = {
    val read = WithClose(reader.features())(f => f.map(ScalaSimpleFeature.copy).toList)
    read.map(_.getID) mustEqual lineFeatures.map(_.getID)
    forall(0 until lineSft.getAttributeCount - 1) { i =>
      read.map(_.getAttribute(i)) mustEqual lineFeatures.map(_.getAttribute(i))
    }
    forall(read.map(_.getDefaultGeometry()).zip(lineFeatures.map(_.getDefaultGeometry))) {
      case (r: LineString, f: LineString) =>
        // because of our limited precision in arrow queries, points don't exactly match up
        r.getNumPoints mustEqual f.getNumPoints
        foreach(0 until r.getNumPoints) { n =>
          r.getCoordinateN(n).x must beCloseTo(f.getCoordinateN(n).x, 0.001)
          r.getCoordinateN(n).y must beCloseTo(f.getCoordinateN(n).y, 0.001)
        }
    }
  }

  step {
    allocator.close()
  }
}
