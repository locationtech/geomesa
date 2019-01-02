/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.process.query

import org.geotools.data.DataUtilities
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.store.ReTypingFeatureCollection
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class QueryProcessTest extends Specification {

  val process = new QueryProcess

  val sft = SimpleFeatureTypes.createType("sample", "track:String,dtg:Date,*geom:Point:srid=4326")

  val fc = new ListFeatureCollection(sft)

  val features = (0 until 10).map { i =>
    val sf = new ScalaSimpleFeature(sft, i.toString)
    sf.setAttribute(0, s"t-${i % 2}")
    sf.setAttribute(1, s"2017-05-24T00:00:0$i.000Z")
    sf.setAttribute(2, s"POINT(45 5$i)")
    sf
  }

  step {
    features.foreach(fc.add)
  }

  "QueryProcess" should {
    "manually visit a feature collection" in {
      val filters = Seq("track = 't-1'", "bbox(geom,44,49,46,52)", "INCLUDE")
      foreach(filters.map(ECQL.toFilter)) { filter =>
        val result = SelfClosingIterator(process.execute(fc, filter, null).features).toSeq
        result mustEqual features.filter(filter.evaluate)
      }
    }
    "manually visit a feature collection and transform" in {
      import scala.collection.JavaConversions._
      val filter = ECQL.toFilter("track = 't-1'")
      val transforms = Seq(Seq("track", "geom"), Seq("geom"))
      foreach(transforms) { transform =>
        val retype = DataUtilities.createSubType(sft, transform.toArray)
        val result = SelfClosingIterator(process.execute(fc, filter, transform).features).toSeq
        result mustEqual SelfClosingIterator(new ReTypingFeatureCollection(fc.subCollection(filter), retype).features()).toSeq
      }
    }
  }
}
