/***********************************************************************
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.process.transform

import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.geotools.data.collection.ListFeatureCollection
import org.junit.runner.RunWith
import org.locationtech.geomesa.arrow.ArrowAllocator
import org.locationtech.geomesa.arrow.io.SimpleFeatureArrowFileReader
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.io.ByteArrayInputStream
import java.util.Collections
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class ArrowConversionProcessTest extends Specification {

  import scala.collection.JavaConverters._

  sequential

  implicit val allocator: BufferAllocator = new RootAllocator(Long.MaxValue)

  val sft = SimpleFeatureTypes.createImmutableType("ArrowConversionProcessTest", "name:String,dtg:Date,*geom:Point:srid=4326")

  val process = new ArrowConversionProcess

  val features = (0 until 10).map { i =>
    ScalaSimpleFeature.create(sft, s"0$i", s"name${i % 2}", s"2017-02-20T00:00:0$i.000Z", s"POINT(40 ${50 + i})")
  }

  val collection = new ListFeatureCollection(sft, new Random(-1L).shuffle(features.asInstanceOf[Seq[SimpleFeature]]).asJava)

  "ArrowConversionProcess" should {
    "encode an empty feature collection" in {
      val bytes = process.execute(new ListFeatureCollection(sft), null, null, null, null, null, null, null).asScala.reduce(_ ++ _)
      WithClose(SimpleFeatureArrowFileReader.streaming(() => new ByteArrayInputStream(bytes))) { reader =>
        reader.sft mustEqual sft
        SelfClosingIterator(reader.features()) must beEmpty
      }
    }

    "encode a generic feature collection" in {
      val bytes = process.execute(collection, null, null, null, null, null, null, null).asScala.reduce(_ ++ _)
      WithClose(SimpleFeatureArrowFileReader.streaming(() => new ByteArrayInputStream(bytes))) { reader =>
        reader.sft mustEqual sft
        SelfClosingIterator(reader.features()).map(ScalaSimpleFeature.copy).toSeq must
            containTheSameElementsAs(features)
      }
    }

    "encode a generic empty feature collection with dictionary values without leaking memory" in {
      // This returns an empty iterator.
      process.execute(new ListFeatureCollection(sft), null, null, null, Collections.singletonList("name"), null, null, null)
      ArrowAllocator.getAllocatedMemory(sft.getTypeName) must equalTo(0)
    }

    "encode a generic feature collection with dictionary values" in {
      val bytes = process.execute(collection, null, null, null, Collections.singletonList("name"), null, null, null).asScala.reduce(_ ++ _)
      WithClose(SimpleFeatureArrowFileReader.streaming(() => new ByteArrayInputStream(bytes))) { reader =>
        reader.sft mustEqual sft
        SelfClosingIterator(reader.features()).map(ScalaSimpleFeature.copy).toSeq must
            containTheSameElementsAs(features)
        reader.dictionaries.get("name") must beSome
      }
    }

    "encode a generic feature collection with sorting" in {
      val ascending = process.execute(collection, null, null, null, null, "dtg", null, null).asScala.reduce(_ ++ _)
      WithClose(SimpleFeatureArrowFileReader.streaming(() => new ByteArrayInputStream(ascending))) { reader =>
        reader.sft mustEqual sft
        SelfClosingIterator(reader.features()).map(ScalaSimpleFeature.copy).toSeq mustEqual features
      }
      val descending = process.execute(collection, null, null, null, null, "dtg", true, null).asScala.reduce(_ ++ _)
      WithClose(SimpleFeatureArrowFileReader.streaming(() => new ByteArrayInputStream(descending))) { reader =>
        reader.sft mustEqual sft
        SelfClosingIterator(reader.features()).map(ScalaSimpleFeature.copy).toSeq mustEqual features.reverse
      }
    }

    "encode a generic feature collection with sorting and dictionary values" in {
      val ascending = process.execute(collection, null, null, null, Collections.singletonList("name"), "dtg", null, null).asScala.reduce(_ ++ _)
      WithClose(SimpleFeatureArrowFileReader.streaming(() => new ByteArrayInputStream(ascending))) { reader =>
        reader.sft mustEqual sft
        SelfClosingIterator(reader.features()).map(ScalaSimpleFeature.copy).toSeq mustEqual features
        reader.dictionaries.get("name") must beSome
      }
      val descending = process.execute(collection, null, null, null, Collections.singletonList("name"), "dtg", true, null).asScala.reduce(_ ++ _)
      WithClose(SimpleFeatureArrowFileReader.streaming(() => new ByteArrayInputStream(descending))) { reader =>
        reader.sft mustEqual sft
        SelfClosingIterator(reader.features()).map(ScalaSimpleFeature.copy).toSeq mustEqual features.reverse
        reader.dictionaries.get("name") must beSome
      }
    }
  }

  step {
    allocator.close()
  }
}
