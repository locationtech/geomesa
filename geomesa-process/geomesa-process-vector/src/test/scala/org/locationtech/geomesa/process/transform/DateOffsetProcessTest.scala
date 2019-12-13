/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.process.transform

import java.util.Date

import org.geotools.data.collection.ListFeatureCollection
import org.geotools.util.Converters
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class DateOffsetProcessTest extends Specification {

  val sft = SimpleFeatureTypes.createType("arrow", "name:String,dtg:Date,*geom:Point:srid=4326")

  def features = (0 until 10).map { i =>
    ScalaSimpleFeature.create(sft, s"0$i", s"name${i % 2}", s"2017-02-20T00:00:0$i.000Z", s"POINT(40 ${50 + i})")
  }

  val process = new DateOffsetProcess

  "DateOffSet Process" should {
    "move a feature collection back in time one day" in {
      val collection = new ListFeatureCollection(sft, new Random(-1L).shuffle(features))
      val result = process.execute(collection, "dtg", "P-1D")
      val dates = SelfClosingIterator(result.features()).map(_.getAttribute("dtg")).toList
      val expected = (0 until 10).map(i => Converters.convert(s"2017-02-19T00:00:0$i.000Z", classOf[Date]))
      dates must containTheSameElementsAs(expected)
    }

    "move a feature collection forward in time one day" in {
      val collection = new ListFeatureCollection(sft, new Random(-1L).shuffle(features))
      val result = process.execute(collection, "dtg", "P1D")
      val dates = SelfClosingIterator(result.features()).map(_.getAttribute("dtg")).toList
      val expected = (0 until 10).map(i => Converters.convert(s"2017-02-21T00:00:0$i.000Z", classOf[Date]))
      dates must containTheSameElementsAs(expected)
    }

    "move a feature collection back in time one hour" in {
      val collection = new ListFeatureCollection(sft, new Random(-1L).shuffle(features))
      val result = process.execute(collection, "dtg", "PT-1H")
      val dates = SelfClosingIterator(result.features()).map(_.getAttribute("dtg")).toList
      val expected = (0 until 10).map(i => Converters.convert(s"2017-02-19T23:00:0$i.000Z", classOf[Date]))
      dates must containTheSameElementsAs(expected)
    }

    "move a feature collection forward with mixed units" in {
      val collection = new ListFeatureCollection(sft, new Random(-1L).shuffle(features))
      val result = process.execute(collection, "dtg", "P5DT1H")
      val dates = SelfClosingIterator(result.features()).map(_.getAttribute("dtg")).toList
      val expected = (0 until 10).map(i => Converters.convert(s"2017-02-25T01:00:0$i.000Z", classOf[Date]))
      dates must containTheSameElementsAs(expected)
    }
  }
}
