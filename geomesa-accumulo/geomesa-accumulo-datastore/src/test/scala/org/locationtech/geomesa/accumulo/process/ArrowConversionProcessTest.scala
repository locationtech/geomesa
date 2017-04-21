/***********************************************************************
* Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.process

import java.io.ByteArrayInputStream

import org.apache.arrow.memory.RootAllocator
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.arrow.io.SimpleFeatureArrowFileReader
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.filter.Filter
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ArrowConversionProcessTest extends TestWithDataStore {

  import scala.collection.JavaConversions._

  sequential

  override val spec = "name:String,dtg:Date,*geom:Point:srid=4326"

  implicit val allocator = new RootAllocator(Long.MaxValue)

  val process = new ArrowConversionProcess

  val features = (0 until 10).map { i =>
    ScalaSimpleFeature.create(sft, s"0$i", s"name${i % 2}", s"2017-02-20T00:00:0$i.000Z", s"POINT(40 ${50 + i})")
  }

  val listCollection = new ListFeatureCollection(sft, features)

  addFeatures(features)

  "ArrowConversionProcess" should {
    "encode an empty feature collection" in {
      val bytes = process.execute(new ListFeatureCollection(sft), null).reduce(_ ++ _)
      WithClose(new SimpleFeatureArrowFileReader(new ByteArrayInputStream(bytes))) { reader =>
        reader.sft mustEqual sft
        reader.features must beEmpty
      }
    }

    "encode a generic feature collection" in {
      val bytes = process.execute(listCollection, null).reduce(_ ++ _)
      WithClose(new SimpleFeatureArrowFileReader(new ByteArrayInputStream(bytes))) { reader =>
        reader.sft mustEqual sft
        reader.features.toSeq must containTheSameElementsAs(features)
      }
    }

    "encode a generic feature collection with dictionary values" in {
      val bytes = process.execute(listCollection, Seq("name")).reduce(_ ++ _)
      WithClose(new SimpleFeatureArrowFileReader(new ByteArrayInputStream(bytes))) { reader =>
        reader.sft mustEqual sft
        reader.features.toSeq must containTheSameElementsAs(features)
        reader.dictionaries.get("name") must beSome
      }
    }.pendingUntilFixed("Can't encode dictionary values for non-distributed query")

    "encode an empty accumulo feature collection" in {
      val bytes = process.execute(fs.getFeatures(ECQL.toFilter("bbox(geom,20,20,30,30)")), null).reduce(_ ++ _)
      WithClose(new SimpleFeatureArrowFileReader(new ByteArrayInputStream(bytes))) { reader =>
        reader.sft mustEqual sft
        reader.features must beEmpty
      }
    }

    "encode an accumulo feature collection in distributed fashion" in {
      val bytes = process.execute(fs.getFeatures(Filter.INCLUDE), null).reduce(_ ++ _)
      WithClose(new SimpleFeatureArrowFileReader(new ByteArrayInputStream(bytes))) { reader =>
        reader.sft mustEqual sft
        reader.features.toSeq must containTheSameElementsAs(features)
      }
    }

    "encode an accumulo feature collection in distributed fashion with dictionary values" in {
      val bytes = process.execute(fs.getFeatures(Filter.INCLUDE), Seq("name")).reduce(_ ++ _)
      WithClose(new SimpleFeatureArrowFileReader(new ByteArrayInputStream(bytes))) { reader =>
        reader.sft mustEqual sft
        reader.features.toSeq must containTheSameElementsAs(features)
        reader.dictionaries.get("name:String") must beSome
      }
    }
  }

  step {
    allocator.close()
  }
}
