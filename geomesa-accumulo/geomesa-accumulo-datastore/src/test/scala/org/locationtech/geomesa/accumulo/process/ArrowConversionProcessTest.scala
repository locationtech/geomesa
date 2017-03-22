/***********************************************************************
* Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.process

import org.geotools.data.collection.ListFeatureCollection
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.opengis.filter.Filter
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ArrowConversionProcessTest extends TestWithDataStore {

  import scala.collection.JavaConversions._

  sequential

  override val spec = "name:String,dtg:Date,*geom:Point:srid=4326"

  val process = new ArrowConversionProcess

  val features = (0 until 10).map { i =>
    ScalaSimpleFeature.create(sft, s"0$i", s"name$i", s"2017-02-20T00:00:0$i.000Z", s"POINT(40 ${50 + i})")
  }

  val listCollection = new ListFeatureCollection(sft, features)

  addFeatures(features)

  "ArrowConversionProcess" should {
    "encode an empty feature collection" in {
      val bytes = process.execute(new ListFeatureCollection(sft), null)
      bytes must beEmpty
    }

    "encode a generic feature collection" in {
      val bytes = process.execute(listCollection, null).toList
      bytes must haveLength(1)
      ok
    }

    "encode a generic feature collection with dictionary values" in {
      val bytes = process.execute(listCollection, Seq("name")).toList
      bytes must haveLength(1)
      ok
    }

    "encode an accumulo feature collection in distributed fashion" in {
      val bytes = process.execute(fs.getFeatures(Filter.INCLUDE), null).toList
      bytes.length must beLessThan(10)
      ok
    }

    "encode an accumulo feature collection in distributed fashion with dictionary values" in {
      val bytes = process.execute(fs.getFeatures(Filter.INCLUDE), Seq("name")).toList
      bytes.length must beLessThan(10)
      ok
    }
  }
}
