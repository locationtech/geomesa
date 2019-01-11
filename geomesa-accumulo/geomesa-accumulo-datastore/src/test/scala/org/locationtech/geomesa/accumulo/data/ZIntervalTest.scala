/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import org.geotools.data.{Query, Transaction}
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithMultipleSfts
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType
import org.opengis.feature.simple.SimpleFeatureType
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ZIntervalTest extends Specification with TestWithMultipleSfts {

  sequential

  val key = SimpleFeatureTypes.Configs.Z3_INTERVAL_KEY
  val spec = "name:String,dtg:Date,*geom:Point:srid=4326"

  val filters = Seq(
    ("2015-01-01T00:00:00.000Z/2015-01-12T00:00:00.000Z", (0 until 10).map(_.toString)),
    ("2015-01-03T00:00:00.000Z/2015-01-09T00:00:00.000Z", (2 until 8).map(_.toString)),
    ("2015-01-01T11:59:59.999Z/2015-01-01T12:00:00.001Z", Seq("0"))
  ).map { case (f, results) => (ECQL.toFilter(s"bbox(geom, -121, 69, -120, 80) AND dtg DURING $f"), results) }

  def addFeatures(sft: SimpleFeatureType): Unit = {
    addFeatures(sft, (0 until 10).map { i =>
      val sf = new ScalaSimpleFeature(sft, s"$i")
      sf.setAttribute("name", "fred")
      sf.setAttribute("dtg", f"2015-01-${i+1}%02dT12:00:00.000Z")
      sf.setAttribute("geom", s"POINT(-120 7$i)")
      sf
    })
  }

  "Z3 Index" should {
    "support configurable time intervals by day" in {
      val sft = createNewSchema(s"$spec;$key='day'")
      addFeatures(sft)
      forall(filters) { case (filter, results) =>
        val query = new Query(sft.getTypeName, filter)
        val features = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
        features.map(_.getID) must containTheSameElementsAs(results)
      }
    }
    "support configurable time intervals by week" in {
      val sft = createNewSchema(s"$spec;$key='week'")
      addFeatures(sft)
      forall(filters) { case (filter, results) =>
        val query = new Query(sft.getTypeName, filter)
        val features = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
        features.map(_.getID) must containTheSameElementsAs(results)
      }
    }
    "support configurable time intervals by month" in {
      val sft = createNewSchema(s"$spec;$key='month'")
      addFeatures(sft)
      forall(filters) { case (filter, results) =>
        val query = new Query(sft.getTypeName, filter)
        val features = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
        features.map(_.getID) must containTheSameElementsAs(results)
      }
    }
    "support configurable time intervals by year" in {
      val sft = createNewSchema(s"$spec;$key='year'")
      addFeatures(sft)
      forall(filters) { case (filter, results) =>
        val query = new Query(sft.getTypeName, filter)
        val features = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
        features.map(_.getID) must containTheSameElementsAs(results)
      }
    }
  }
}
