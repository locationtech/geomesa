/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.process.transform

import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.geotools.api.filter.Filter
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithFeatureType
import org.locationtech.geomesa.arrow.io.SimpleFeatureArrowFileReader
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.runner.JUnitRunner

import java.io.ByteArrayInputStream
import java.util.Collections

@RunWith(classOf[JUnitRunner])
class ArrowConversionProcessTest extends TestWithFeatureType {

  import scala.collection.JavaConverters._

  sequential

  override val spec = "name:String:index=join,team:String,dtg:Date,*geom:Point:srid=4326"

  implicit val allocator: BufferAllocator = new RootAllocator(Long.MaxValue)

  val process = new ArrowConversionProcess

  val features = (0 until 10).map { i =>
    ScalaSimpleFeature.create(sft, s"0$i", s"name${i % 2}", s"team$i", s"2017-02-20T00:00:0$i.000Z", s"POINT(40 ${50 + i})")
  }

  addFeatures(features)

  "ArrowConversionProcess" should {
    "encode an empty feature collection" in {
      val bytes = process.execute(new ListFeatureCollection(sft), null, null, null, null, null, null, null, null).asScala.reduce(_ ++ _)
      WithClose(SimpleFeatureArrowFileReader.streaming(() => new ByteArrayInputStream(bytes))) { reader =>
        reader.sft mustEqual sft
        SelfClosingIterator(reader.features()) must beEmpty
      }
    }

    "encode an empty accumulo feature collection" in {
      val bytes = process.execute(fs.getFeatures(ECQL.toFilter("bbox(geom,20,20,30,30)")), null, null, null, null, null, null, null, null).asScala.reduce(_ ++ _)
      WithClose(SimpleFeatureArrowFileReader.streaming(() => new ByteArrayInputStream(bytes))) { reader =>
        reader.sft mustEqual sft
        SelfClosingIterator(reader.features()) must beEmpty
      }
    }

    "encode an accumulo feature collection in distributed fashion" in {
      val bytes = process.execute(fs.getFeatures(Filter.INCLUDE), null, null, null, null, null, null, null, null).asScala.reduce(_ ++ _)
      WithClose(SimpleFeatureArrowFileReader.streaming(() => new ByteArrayInputStream(bytes))) { reader =>
        reader.sft mustEqual sft
        SelfClosingIterator(reader.features()).map(ScalaSimpleFeature.copy).toSeq must
            containTheSameElementsAs(features)
      }
    }

    "encode an accumulo feature collection in distributed fashion with calculated dictionary values" in {
      val filter = ECQL.toFilter("name = 'name0'")
      val bytes = process.execute(fs.getFeatures(filter), null, null, null, Collections.singletonList("name"), null, null, null, null).asScala.reduce(_ ++ _)
      WithClose(SimpleFeatureArrowFileReader.streaming(() => new ByteArrayInputStream(bytes))) { reader =>
        reader.sft mustEqual sft
        SelfClosingIterator(reader.features()).map(ScalaSimpleFeature.copy).toSeq must
            containTheSameElementsAs(features.filter(filter.evaluate))
        // verify only exact values were used for the dictionary
        reader.dictionaries.map { case (k, v) => (k, v.iterator.toSeq) } mustEqual Map("name" -> Seq("name0"))
      }
    }

    "sort and encode an accumulo feature collection in distributed fashion" in {
      val bytes = process.execute(fs.getFeatures(Filter.INCLUDE), null, null, null, null, "dtg", null, null, null).asScala.reduce(_ ++ _)
      WithClose(SimpleFeatureArrowFileReader.streaming(() => new ByteArrayInputStream(bytes))) { reader =>
        reader.sft mustEqual sft
        SelfClosingIterator(reader.features()).map(ScalaSimpleFeature.copy).toList mustEqual features
      }
    }

    "reverse sort and encode an accumulo feature collection in distributed fashion" in {
      val bytes = process.execute(fs.getFeatures(Filter.INCLUDE), null, null, null, null, "dtg", Boolean.box(true), null, null).asScala.reduce(_ ++ _)
      WithClose(SimpleFeatureArrowFileReader.streaming(() => new ByteArrayInputStream(bytes))) { reader =>
        reader.sft mustEqual sft
        SelfClosingIterator(reader.features()).map(ScalaSimpleFeature.copy).toList mustEqual features.reverse
      }
    }
  }

  step {
    allocator.close()
  }
}
