/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import java.time.{ZoneOffset, ZonedDateTime}
import java.util.Date

import org.geotools.data._
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.factory.Hints
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.date.DateUtils.toInstant
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.opengis.filter.Filter
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class AccumuloDataStoreAlterSchemaTest extends TestWithDataStore {

  sequential

  override val spec = "dtg:Date,*geom:Point:srid=4326"

  step {
    addFeatures {
      (0 until 10).filter(_ % 2 == 0).map { i =>
        val sf = new ScalaSimpleFeature(sft, s"f$i")
        sf.setAttribute(0, s"2014-01-01T0$i:00:00.000Z")
        sf.setAttribute(1, s"POINT(5$i 50)")
        sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        sf
      }
    }

    val builder = new SimpleFeatureTypeBuilder()
    builder.init(sft)
    builder.userData("index", "join")
    builder.add("attr1", classOf[String])
    val updatedSft = builder.buildFeatureType()
    updatedSft.getUserData.putAll(sft.getUserData)

    ds.updateSchema(sftName, updatedSft)

    // use a new data store to avoid cached sft issues in the stat serializer
    DataStoreFinder.getDataStore(dsParams).getFeatureSource(sftName).asInstanceOf[SimpleFeatureStore].addFeatures {
      val sft = ds.getSchema(sftName)
      val collection = new DefaultFeatureCollection()
      collection.addAll {
        (0 until 10).filter(_ % 2 == 1).map { i =>
          val sf = new ScalaSimpleFeature(sft, s"f$i")
          sf.setAttribute(0, s"2014-01-01T0$i:00:00.000Z")
          sf.setAttribute(1, s"POINT(5$i 50)")
          sf.setAttribute(2, s"$i")
          sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
          sf
        }
      }
      collection
    }
  }

  "AccumuloDataStore" should {
    "work with an altered schema" >> {
      val query = new Query(sftName, Filter.INCLUDE)
      val features = SelfClosingIterator(ds.getFeatureSource(sftName).getFeatures(query).features).toList
      features.map(_.getID) must containTheSameElementsAs((0 until 10).map("f" + _))
      forall(features)(_.getAttribute("dtg") must not(beNull))
      forall(features)(_.getAttribute("geom") must not(beNull))
      forall(features.filter(_.getID.substring(1).toInt % 2 == 0))(_.getAttribute("attr1") must beNull)
      forall(features.filter(_.getID.substring(1).toInt % 2 == 1))(_.getAttribute("attr1") must not(beNull))
    }
    "handle transformations to updated types" >> {
      "for old attributes with new and old features" >> {
        val query = new Query(sftName, ECQL.toFilter("IN ('f1', 'f2')"), Array("geom", "dtg"))
        val features = SelfClosingIterator(ds.getFeatureSource(sftName).getFeatures(query).features).toList
        features.map(_.getID) must containTheSameElementsAs(Seq("f1", "f2"))
        features.sortBy(_.getID).map(_.getAttribute("geom").toString) mustEqual Seq("POINT (51 50)", "POINT (52 50)")
        features.sortBy(_.getID).map(_.getAttribute("dtg"))
            .map(d => ZonedDateTime.ofInstant(toInstant(d.asInstanceOf[Date]), ZoneOffset.UTC).getHour) mustEqual Seq(1, 2)
      }
      "for old attributes with new features" >> {
        val query = new Query(sftName, ECQL.toFilter("IN ('f1')"), Array("geom", "dtg"))
        val features = SelfClosingIterator(ds.getFeatureSource(sftName).getFeatures(query).features).toList
        features.map(_.getID) must containTheSameElementsAs(Seq("f1"))
        features.head.getAttribute("geom").toString mustEqual "POINT (51 50)"
        ZonedDateTime.ofInstant(toInstant(features.head.getAttribute("dtg").asInstanceOf[Date]), ZoneOffset.UTC).getHour mustEqual 1
      }
      "for old attributes with old features" >> {
        val query = new Query(sftName, ECQL.toFilter("IN ('f2')"), Array("geom", "dtg"))
        val features = SelfClosingIterator(ds.getFeatureSource(sftName).getFeatures(query).features).toList
        features.map(_.getID) must containTheSameElementsAs(Seq("f2"))
        features.head.getAttribute("geom").toString mustEqual "POINT (52 50)"
        ZonedDateTime.ofInstant(toInstant(features.head.getAttribute("dtg").asInstanceOf[Date]), ZoneOffset.UTC).getHour mustEqual 2
      }
      "for new attributes with new and old features" >> {
        val query = new Query(sftName, ECQL.toFilter("IN ('f1', 'f2')"), Array("geom", "attr1"))
        val features = SelfClosingIterator(ds.getFeatureSource(sftName).getFeatures(query).features).toList
        features.map(_.getID) must containTheSameElementsAs(Seq("f1", "f2"))
        features.sortBy(_.getID).map(_.getAttribute("geom").toString) mustEqual Seq("POINT (51 50)", "POINT (52 50)")
        features.sortBy(_.getID).map(_.getAttribute("attr1")) mustEqual Seq("1", null)
      }
      "for new attributes with new features" >> {
        val query = new Query(sftName, ECQL.toFilter("IN ('f1')"), Array("geom", "attr1"))
        val features = SelfClosingIterator(ds.getFeatureSource(sftName).getFeatures(query).features).toList
        features.map(_.getID) must containTheSameElementsAs(Seq("f1"))
        features.head.getAttribute("geom").toString mustEqual "POINT (51 50)"
        features.head.getAttribute("attr1") mustEqual "1"
      }
      "for new attributes with old features" >> {
        val query = new Query(sftName, ECQL.toFilter("IN ('f2')"), Array("geom", "attr1"))
        val features = SelfClosingIterator(ds.getFeatureSource(sftName).getFeatures(query).features).toList
        features.map(_.getID) must containTheSameElementsAs(Seq("f2"))
        features.head.getAttribute("geom").toString mustEqual "POINT (52 50)"
        features.head.getAttribute("attr1") must beNull
      }
    }
  }
}
