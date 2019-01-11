/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.process.analytic

import org.geotools.data.collection.ListFeatureCollection
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class UniqueProcessTest extends Specification {

  val process = new UniqueProcess

  val sft = SimpleFeatureTypes.createType("unique", "track:String,dtg:Date,*geom:Point:srid=4326")

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

  "UniqueProcess" should {
    "manually visit a feature collection" in {
      val result = SelfClosingIterator(process.execute(fc, "track", null, null, null, null, null).features).toSeq
      foreach(result)(_.getAttributeCount mustEqual 1)
      result.map(_.getAttribute(0)) must containTheSameElementsAs(Seq("t-0", "t-1"))
    }
    "manually visit a feature collection with counts" in {
      val result = SelfClosingIterator(process.execute(fc, "track", null, true, null, null, null).features).toSeq
      foreach(result)(_.getAttributeCount mustEqual 2)
      result.map(_.getAttribute(0)) must containTheSameElementsAs(Seq("t-0", "t-1"))
      result.map(_.getAttribute(1)) must containTheSameElementsAs(Seq(5L, 5L))
    }
    "manually visit a feature collection with a filter" in {
      val result = SelfClosingIterator(process.execute(fc, "track", ECQL.toFilter("dtg before 2017-05-24T00:00:05.001Z"), true, null, null, null).features).toSeq
      foreach(result)(_.getAttributeCount mustEqual 2)
      result.map(_.getAttribute(0)) must containTheSameElementsAs(Seq("t-0", "t-1"))
      result.map(_.getAttribute(1)) must containTheSameElementsAs(Seq(3L, 3L))
    }
  }
}
