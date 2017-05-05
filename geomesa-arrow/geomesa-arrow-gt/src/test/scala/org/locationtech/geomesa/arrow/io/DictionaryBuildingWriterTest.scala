/***********************************************************************
* Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.arrow.io

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import org.apache.arrow.memory.RootAllocator
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.SimpleFeature
import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DictionaryBuildingWriterTest extends Specification {

  val sft = SimpleFeatureTypes.createType("test", "name:String,dtg:Date,*geom:Point:srid=4326")
  val features = (0 until 10).map { i =>
    ScalaSimpleFeature.create(sft, s"0$i", s"name0${i % 2}", s"2017-03-15T00:0$i:00.000Z", s"POINT (4$i 5$i)")
  }

  implicit val allocator = new RootAllocator(Long.MaxValue)

  // note: need to compare as we iterate since values are only valid until 'next'
  def compare(features: Iterator[SimpleFeature], expected: Seq[SimpleFeature]): MatchResult[Any] = {
    var i = 0
    while (features.hasNext) {
      features.next mustEqual expected(i)
      i += 1
    }
    i mustEqual expected.length
  }

  "SimpleFeatureVector" should {
    "dynamically encode dictionary values" >> {
      val out = new ByteArrayOutputStream()
      WithClose(DictionaryBuildingWriter.create(sft, Seq("name"))) { writer =>
        features.foreach(writer.add)
        writer.encode(out)
      }
      WithClose(SimpleFeatureArrowFileReader.streaming(() => new ByteArrayInputStream(out.toByteArray))) { reader =>
        reader.dictionaries must haveSize(1)
        reader.dictionaries.get("name:String") must beSome
        reader.dictionaries("name:String").values must containTheSameElementsAs(Seq("name00", "name01"))

        WithClose(reader.features())(f => compare(f, features))
      }
    }
  }

  step {
    allocator.close()
  }
}
