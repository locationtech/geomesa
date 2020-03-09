/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import org.geotools.data._
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithFeatureType
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.sft.SimpleFeatureSpecParser
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AccumuloDataStoreAlterSchemaTest extends TestWithFeatureType {

  sequential

  override val spec = "name:String:index=true,dtg:Date,*geom:Point:srid=4326"

  def filters(name: String = "name") = Seq(
    s"$name = 'name0' OR $name = 'name1'",
    "bbox(geom,38,53,42,57)",
    "bbox(geom,38,53,42,57) AND dtg during 2018-01-01T00:00:00.000Z/2018-01-01T12:00:00.000Z",
    "IN ('0', '1')"
  ).map(ECQL.toFilter)

  lazy val feature = ScalaSimpleFeature.create(sft, "0", "name0", "2018-01-01T06:00:00.000Z", "POINT (40 55)")
  lazy val feature2 = ScalaSimpleFeature.create(ds.getSchema(sftName), "1", "name1", "2018-01-01T06:01:00.000Z", "POINT (41 55)", "1")

  "AccumuloDataStore" should {
    "add features and query them" in {
      addFeatures(Seq(feature))
      forall(filters()) { filter =>
        val reader = ds.getFeatureReader(new Query(sft.getTypeName, filter), Transaction.AUTO_COMMIT)
        SelfClosingIterator(reader).toList mustEqual Seq(feature)
      }
      ds.stats.getCount(sft) must beSome(1L)
    }
    "rename schema and still query features" in {
      // rename
      val update = SimpleFeatureTypes.renameSft(this.sft, "rename")
      ds.updateSchema(this.sft.getTypeName, update)

      val sft = ds.getSchema("rename")
      sft must not(beNull)
      sft.getTypeName mustEqual "rename"
      ds.getSchema(sftName) must beNull

      forall(filters()) { filter =>
        val reader = ds.getFeatureReader(new Query(sft.getTypeName, filter), Transaction.AUTO_COMMIT)
        SelfClosingIterator(reader).toList mustEqual Seq(ScalaSimpleFeature.copy(sft, feature))
      }
      ds.stats.getCount(sft) must beSome(1L)
      ds.stats.getMinMax[String](sft, "name", exact = false).map(_.max) must beSome("name0")
    }
    "rename column and still query features" in {
      var sft = ds.getSchema("rename")
      val builder = new SimpleFeatureTypeBuilder()
      builder.init(sft)
      builder.set(0, SimpleFeatureSpecParser.parseAttribute("names:String:index=true").toDescriptor)
      val update = builder.buildFeatureType()
      update.getUserData.putAll(sft.getUserData)
      ds.updateSchema("rename", update)

      sft = ds.getSchema("rename")
      sft must not(beNull)
      sft.getDescriptor(0).getLocalName mustEqual "names"
      sft.getDescriptor("names") mustEqual sft.getDescriptor(0)

      forall(filters("names")) { filter =>
        val reader = ds.getFeatureReader(new Query(sft.getTypeName, filter), Transaction.AUTO_COMMIT)
        SelfClosingIterator(reader).toList mustEqual Seq(ScalaSimpleFeature.copy(sft, feature))
      }
      ds.stats.getCount(sft) must beSome(1L)
      ds.stats.getMinMax[String](sft, "names", exact = false).map(_.max) must beSome("name0")
    }
    "rename column a second time and still query features" in {
      var sft = ds.getSchema("rename")
      val builder = new SimpleFeatureTypeBuilder()
      builder.init(sft)
      builder.set(0, SimpleFeatureSpecParser.parseAttribute("nam:String:index=true").toDescriptor)
      val update = builder.buildFeatureType()
      update.getUserData.putAll(sft.getUserData)
      ds.updateSchema("rename", update)

      sft = ds.getSchema("rename")
      sft must not(beNull)
      sft.getDescriptor(0).getLocalName mustEqual "nam"
      sft.getDescriptor("nam") mustEqual sft.getDescriptor(0)

      forall(filters("nam")) { filter =>
        val reader = ds.getFeatureReader(new Query(sft.getTypeName, filter), Transaction.AUTO_COMMIT)
        SelfClosingIterator(reader).toList mustEqual Seq(ScalaSimpleFeature.copy(sft, feature))
      }
      ds.stats.getCount(sft) must beSome(1L)
      ds.stats.getMinMax[String](sft, "nam", exact = false).map(_.max) must beSome("name0")
    }
    "rename type and column and still query features" in {
      var sft = ds.getSchema("rename")
      val builder = new SimpleFeatureTypeBuilder()
      builder.init(sft)
      builder.set(0, SimpleFeatureSpecParser.parseAttribute("n:String").toDescriptor)
      builder.setName("foo")
      val update = builder.buildFeatureType()
      update.getUserData.putAll(sft.getUserData)
      ds.updateSchema("rename", update)

      sft = ds.getSchema("foo")
      sft must not(beNull)
      sft.getTypeName mustEqual "foo"
      sft.getDescriptor(0).getLocalName mustEqual "n"
      sft.getDescriptor("n") mustEqual sft.getDescriptor(0)

      forall(filters("n")) { filter =>
        val reader = ds.getFeatureReader(new Query(sft.getTypeName, filter), Transaction.AUTO_COMMIT)
        SelfClosingIterator(reader).toList mustEqual Seq(ScalaSimpleFeature.copy(sft, feature))
      }
      ds.stats.getCount(sft) must beSome(1L)
      ds.stats.getMinMax[String](sft, "n", exact = false).map(_.max) must beSome("name0")
    }
    "alter schema by adding a new column with an index" in {
      var sft = ds.getSchema("foo")
      val builder = new SimpleFeatureTypeBuilder()
      builder.init(sft)
      builder.userData("index", "join")
      builder.add("age", classOf[java.lang.Integer])
      builder.setName(sftName)
      val update = builder.buildFeatureType()
      update.getUserData.putAll(sft.getUserData)

      ds.updateSchema("foo", update)
      sft = ds.getSchema(sftName)
      sft must not(beNull)
      sft.getTypeName mustEqual sftName
      sft.getAttributeCount mustEqual 4
      sft.getDescriptor(3).getLocalName mustEqual "age"
      sft.getDescriptor("age").getType.getBinding mustEqual classOf[java.lang.Integer]
    }
    "add features and query them after altering schema columns" in {
      addFeatures(Seq(feature2))
      foreach(filters("n")) { filter =>
        val query = new Query(sftName, filter)
        val features = SelfClosingIterator(ds.getFeatureSource(sftName).getFeatures(query).features).toList.sortBy(_.getID)
        features.map(_.getID) mustEqual Seq("0", "1")
        features.map(_.getAttribute("dtg")) mustEqual Seq(feature, feature2).map(_.getAttribute("dtg"))
        features.map(_.getAttribute("geom")) mustEqual Seq(feature, feature2).map(_.getAttribute("geom"))
        features.map(_.getAttribute("age")) mustEqual Seq(feature, feature2).map(_.getAttribute("age"))
      }
    }
    "handle transformations for old attributes with new and old features" in {
      foreach(filters("n")) { filter =>
        val query = new Query(sftName, filter, Array("geom", "dtg"))
        val features = SelfClosingIterator(ds.getFeatureSource(sftName).getFeatures(query).features).toList.sortBy(_.getID)
        features.map(_.getID) mustEqual Seq("0", "1")
        features.map(_.getAttribute("geom")) mustEqual Seq(feature, feature2).map(_.getAttribute("geom"))
      }
    }
    "handle transformations for new attributes with new and old features" >> {
      foreach(filters("n")) { filter =>
        val query = new Query(sftName, filter, Array("geom", "age"))
        val features = SelfClosingIterator(ds.getFeatureSource(sftName).getFeatures(query).features).toList.sortBy(_.getID)
        features.map(_.getID) mustEqual Seq("0", "1")
        features.map(_.getAttribute("geom")) mustEqual Seq(feature, feature2).map(_.getAttribute("geom"))
        features.map(_.getAttribute("age")) mustEqual Seq(feature, feature2).map(_.getAttribute("age"))
      }
    }
  }
}
