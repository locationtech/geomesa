/***********************************************************************
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import org.geotools.data._
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithFeatureType
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AccumuloDataStoreJsonTest extends Specification with TestWithFeatureType {

  sequential

  override val spec = "json:String:json=true,*geom:Point:srid=4326"

  def getJson(x: Double, y: Double, props: String = "{}"): String = {
    s"""{"type":"Feature","geometry":{"type":"Point","coordinates":[$x,$y]},"properties":$props}"""
  }

  val sf0 = new ScalaSimpleFeature(sft, "0")
  sf0.setAttribute(0, getJson(45, 60, """{"id":"zero","names":["zero","zilch","nada"]}"""))
  sf0.setAttribute(1, "POINT(45 60)")

  val sf1 = new ScalaSimpleFeature(sft, "1")
  sf1.setAttribute(0, getJson(45, 61, """{"id":"one","names":["solo","top dog"]}"""))
  sf1.setAttribute(1, "POINT(45 61)")

  val sf2 = new ScalaSimpleFeature(sft, "2")
  sf2.setAttribute(0, getJson(45, 62, """{"id":"two","characteristics":{"height":20,"weight":200}}"""))
  sf2.setAttribute(1, "POINT(45 62)")

  val sf3 = new ScalaSimpleFeature(sft, "3")
  sf3.setAttribute(0, getJson(45, 63, """{"id":"three","characteristics":{"height":30,"weight":300}}"""))
  sf3.setAttribute(1, "POINT(45 63)")

  val sf4 = new ScalaSimpleFeature(sft, "4")
  sf4.setAttribute(0, """["a1","a2","a3"]""")
  sf4.setAttribute(1, "POINT(45 63)")

  addFeatures(Seq(sf0, sf1, sf2, sf3, sf4))

  "AccumuloDataStore" should {
    "support json attributes" in {
      import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
      ds.getSchema(sftName).getDescriptor(0).isJson must beTrue
    }
    "support queries against json attributes" in {
      val query = new Query(sftName, ECQL.toFilter(""""$.json.properties.characteristics.height" = 30"""))
      val features = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
      features must haveLength(1)
      features.head.getID mustEqual "3"
      features.head.getAttributes mustEqual sf3.getAttributes // note: whitespace will be stripped from json string
    }
    "support queries against json arrays" in {
      val query = new Query(sftName, ECQL.toFilter(""""$.json[0]" = 'a1'"""))
      val features = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
      features must haveLength(1)
      features.head.getID mustEqual "4"
      features.head.getAttributes mustEqual sf4.getAttributes // note: whitespace will be stripped from json string
    }
    "support projecting schemas" in {
      val query = new Query(sftName, Filter.INCLUDE, "geom", """"$.json.properties.characteristics.height"""")
      val features = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
      features must haveLength(5)
      features.map(_.getAttribute(1)) must containTheSameElementsAs(Seq("20", "30", null, null, null))
    }
    "support projecting json arrays" in {
      val query = new Query(sftName, Filter.INCLUDE, "geom", """"$.json[1]"""")
      val features = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
      features must haveLength(5)
      features.map(_.getAttribute(1)) must containTheSameElementsAs(Seq(null, null, null, null, "a2"))
    }
    "support querying against projected schemas" in {
      val filter = ECQL.toFilter(""""$.json.properties.characteristics.height" = 30""")
      val query = new Query(sftName, filter, "geom", """"$.json.properties.characteristics.height"""")
      val features = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
      features must haveLength(1)
      features.head.getID mustEqual "3"
      features.head.getAttribute(1) mustEqual "30"
    }
  }
}
