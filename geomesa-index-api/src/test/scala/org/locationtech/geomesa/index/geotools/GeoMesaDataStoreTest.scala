/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.geotools

import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureSource}
import org.geotools.data.store.{ReTypingFeatureCollection, ReprojectingFeatureCollection}
import org.geotools.data.{DataStore, Query, Transaction}
import org.geotools.feature.collection.{DecoratingFeatureCollection, DecoratingSimpleFeatureCollection}
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.feature.visitor.CalcResult
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.{JTS, ReferencedEnvelope}
import org.geotools.referencing.CRS
import org.geotools.util.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.TestGeoMesaDataStore
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreTest.{TestFeatureCollection, TestQueryInterceptor, TestSimpleFeatureCollection, TestVisitor}
import org.locationtech.geomesa.index.index.attribute.AttributeIndex
import org.locationtech.geomesa.index.index.id.IdIndex
import org.locationtech.geomesa.index.index.z3.Z3Index
import org.locationtech.geomesa.index.planning.QueryInterceptor
import org.locationtech.geomesa.index.process.GeoMesaProcessVisitor
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs
import org.locationtech.geomesa.utils.geotools.sft.SimpleFeatureSpecParser
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.jts.geom.{Geometry, Point}
import org.opengis.feature.Feature
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GeoMesaDataStoreTest extends Specification {

  import org.locationtech.geomesa.utils.geotools.CRS_EPSG_4326

  val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")

  val ds = new TestGeoMesaDataStore(true)
  ds.createSchema(sft)

  val features = Seq.tabulate(10) { i =>
    ScalaSimpleFeature.create(sft, s"$i", s"name$i", i, f"2018-01-01T$i%02d:00:00.000Z", s"POINT (4$i 55)")
  }

  val epsg3857 = CRS.decode("EPSG:3857")

  step {
    features.foreach(_.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE))
    ds.getFeatureSource(sft.getTypeName).addFeatures(new ListFeatureCollection(sft, features.toArray[SimpleFeature]))
  }

  "GeoMesaDataStore" should {
    "reproject geometries" in {
      val query = new Query("test")
      query.setCoordinateSystemReproject(epsg3857)
      val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toSeq
      results must haveLength(10)

      val transform = CRS.findMathTransform(epsg3857, CRS_EPSG_4326, true)

      foreach(results) { result =>
        result.getFeatureType.getGeometryDescriptor.getCoordinateReferenceSystem mustEqual epsg3857
        val recovered = JTS.transform(result.getDefaultGeometry.asInstanceOf[Geometry], transform).asInstanceOf[Point]
        val expected = features.find(_.getID == result.getID).get.getDefaultGeometry.asInstanceOf[Point]
        recovered.getX must beCloseTo(expected.getX, 0.001)
        recovered.getY must beCloseTo(expected.getY, 0.001)
      }
    }
    "handle weird idl-wrapping polygons" in {
      val filter = ECQL.toFilter("intersects(geom, 'POLYGON((-179.99 45, -179.99 90, 179.99 90, 179.99 45, -179.99 45))')")
      val query = new Query("test", filter)
      val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toSeq
      results must beEmpty
    }
    "throw an exception on invalid attributes" in {
      val filters = Seq(
        "names = 'foo'",
        "bbox(g,-10,-10,10,10)",
        "foo DURING 2018-01-01T00:00:00.000Z/2018-01-01T12:00:00.000Z",
        "bbox(geom,-10,-10,10,10) AND foo DURING 2018-01-01T00:00:00.000Z/2018-01-01T12:00:00.000Z"
      )
      foreach(filters) { filter =>
        val query = new Query("test", ECQL.toFilter(filter))
        ds.getFeatureReader(query, Transaction.AUTO_COMMIT) must throwAn[IllegalArgumentException]
      }
    }
    "intercept and rewrite queries" in {
      val sft = SimpleFeatureTypes.createType("rewrite", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
      sft.getUserData.put(SimpleFeatureTypes.Configs.QueryInterceptors, classOf[TestQueryInterceptor].getName)

      val ds = new TestGeoMesaDataStore(true)
      ds.createSchema(sft)

      ds.getFeatureSource(sft.getTypeName).addFeatures(new ListFeatureCollection(sft, features.toArray[SimpleFeature]))

      // INCLUDE should be re-written to EXCLUDE
      ds.getQueryPlan(new Query(sft.getTypeName)) must beEmpty
      var results = SelfClosingIterator(ds.getFeatureReader(new Query(sft.getTypeName), Transaction.AUTO_COMMIT)).toSeq
      results must beEmpty

      // other queries should go through as normal
      results = SelfClosingIterator(ds.getFeatureReader(new Query(sft.getTypeName, ECQL.toFilter("bbox(geom,39,54,51,56)")), Transaction.AUTO_COMMIT)).toSeq
      results must haveLength(10)
    }
    "update schemas" in {
      foreach(Seq(true, false)) { partitioning =>
        val ds = new TestGeoMesaDataStore(true)
        val spec = "name:String:index=true,age:Int,dtg:Date,*geom:Point:srid=4326;"
        if (partitioning) {
          ds.createSchema(SimpleFeatureTypes.createType("test", s"$spec${Configs.TablePartitioning}=time"))
        } else {
          ds.createSchema(SimpleFeatureTypes.createType("test", spec))
        }

        var sft = ds.getSchema("test")
        val feature = ScalaSimpleFeature.create(sft, "0", "name0", 0, "2018-01-01T06:00:00.000Z", "POINT (40 55)")
        WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          FeatureUtils.write(writer, feature, useProvidedFid = true)
        }

        var filters = Seq(
          "name = 'name0'",
          "bbox(geom,38,53,42,57)",
          "bbox(geom,38,53,42,57) AND dtg during 2018-01-01T00:00:00.000Z/2018-01-01T12:00:00.000Z",
          "IN ('0')"
        ).map(ECQL.toFilter)

        forall(filters) { filter =>
          val reader = ds.getFeatureReader(new Query(sft.getTypeName, filter), Transaction.AUTO_COMMIT)
          SelfClosingIterator(reader).toList mustEqual Seq(feature)
        }
        ds.stats.getCount(sft) must beSome(1L)

        // rename
        ds.updateSchema("test", SimpleFeatureTypes.renameSft(sft, "rename"))

        ds.getSchema("test") must beNull

        sft = ds.getSchema("rename")

        sft must not(beNull)
        sft .getTypeName mustEqual "rename"

        forall(filters) { filter =>
          val reader = ds.getFeatureReader(new Query(sft.getTypeName, filter), Transaction.AUTO_COMMIT)
          SelfClosingIterator(reader).toList mustEqual Seq(ScalaSimpleFeature.copy(sft, feature))
        }
        ds.stats.getCount(sft) must beSome(1L)
        ds.stats.getMinMax[String](sft, "name", exact = false).map(_.max) must beSome("name0")

        // rename column
        Some(new SimpleFeatureTypeBuilder()).foreach { builder =>
          builder.init(ds.getSchema("rename"))
          builder.set(0, SimpleFeatureSpecParser.parseAttribute("names:String:index=true").toDescriptor)
          val update = builder.buildFeatureType()
          update.getUserData.putAll(ds.getSchema("rename").getUserData)
          ds.updateSchema("rename", update)
        }

        sft = ds.getSchema("rename")
        sft must not(beNull)
        sft.getTypeName mustEqual "rename"
        sft.getDescriptor(0).getLocalName mustEqual "names"
        sft.getDescriptor("names") mustEqual sft.getDescriptor(0)

        filters = Seq(ECQL.toFilter("names = 'name0'")) ++ filters.drop(1)

        forall(filters) { filter =>
          val reader = ds.getFeatureReader(new Query(sft.getTypeName, filter), Transaction.AUTO_COMMIT)
          SelfClosingIterator(reader).toList mustEqual Seq(ScalaSimpleFeature.copy(sft, feature))
        }
        ds.stats.getCount(sft) must beSome(1L)
        ds.stats.getMinMax[String](sft, "names", exact = false).map(_.max) must beSome("name0")

        // rename type and column
        Some(new SimpleFeatureTypeBuilder()).foreach { builder =>
          builder.init(ds.getSchema("rename"))
          builder.set(0, SimpleFeatureSpecParser.parseAttribute("n:String").toDescriptor)
          builder.setName("foo")
          val update = builder.buildFeatureType()
          update.getUserData.putAll(ds.getSchema("rename").getUserData)
          ds.updateSchema("rename", update)
        }

        sft = ds.getSchema("foo")
        sft must not(beNull)
        sft.getTypeName mustEqual "foo"
        sft.getDescriptor(0).getLocalName mustEqual "n"
        sft.getDescriptor("n") mustEqual sft.getDescriptor(0)
        ds.getSchema("rename") must beNull

        filters = Seq(ECQL.toFilter("n = 'name0'")) ++ filters.drop(1)

        forall(filters) { filter =>
          val reader = ds.getFeatureReader(new Query(sft.getTypeName, filter), Transaction.AUTO_COMMIT)
          SelfClosingIterator(reader).toList mustEqual Seq(ScalaSimpleFeature.copy(sft, feature))
        }
        ds.stats.getCount(sft) must beSome(1L)
        ds.stats.getMinMax[String](sft, "n", exact = false).map(_.max) must beSome("name0")
      }
    }
    "unwrap decorating feature collections" in {
      val fc = ds.getFeatureSource(sft.getTypeName).getFeatures()
      val collections = Seq(
        new ReTypingFeatureCollection(fc, SimpleFeatureTypes.renameSft(sft, "foo")),
        new ReprojectingFeatureCollection(fc, epsg3857),
        new TestFeatureCollection(fc),
        new TestSimpleFeatureCollection(fc)
      )
      foreach(collections) { collection =>
        val visitor = new TestVisitor()
        GeoMesaFeatureCollection.visit(collection, visitor)
        visitor.visited must beFalse
        visitor.executed must beTrue
      }
    }
    "support timestamp types with stats" in {
      val sft = SimpleFeatureTypes.createType("ts", "dtg:Timestamp,*geom:Point:srid=4326")
      ds.createSchema(sft)
      val feature = ScalaSimpleFeature.create(sft, "0", "2020-01-20T00:00:00.000Z", "POINT (45 55)")
      WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
        FeatureUtils.write(writer, feature, useProvidedFid = true)
      }
      SelfClosingIterator(ds.getFeatureReader(new Query(sft.getTypeName), Transaction.AUTO_COMMIT)).toList mustEqual
          Seq(feature)
      ds.stats.getBounds(sft) mustEqual
          new ReferencedEnvelope(feature.getDefaultGeometry.asInstanceOf[Point].getEnvelopeInternal, CRS_EPSG_4326)
    }
    "prioritize temporal filter plans" in {
      val spec = "name:String:index=true,age:Int,dtg:Date,*geom:Point:srid=4326"

      val ds = new TestGeoMesaDataStore(true)
      ds.createSchema(SimpleFeatureTypes.createType("default", spec))
      ds.createSchema(SimpleFeatureTypes.createType("temporal", s"$spec;geomesa.temporal.priority=true"))

      val filters = Seq(
        ("name like 'a%' and dtg DURING 2020-01-01T00:00:00.00Z/2020-01-02T00:00:00.00Z", AttributeIndex, Z3Index),
        ("name > 'a' AND name < 'b' and dtg DURING 2020-01-01T00:00:00.00Z/2020-01-02T00:00:00.00Z", AttributeIndex, Z3Index),
        ("name = 'a' and dtg DURING 2020-01-01T00:00:00.00Z/2020-01-02T00:00:00.00Z", AttributeIndex, AttributeIndex),
        ("IN('0') and bbox(geom,-10,-10,10,10) and dtg DURING 2020-01-01T00:00:00.00Z/2020-01-02T00:00:00.00Z", IdIndex, IdIndex)
      )

      foreach(filters) { case (f, default, temporal) =>
        ds.getQueryPlan(new Query("default", ECQL.toFilter(f))).map(_.filter.index.name) mustEqual Seq(default.name)
        ds.getQueryPlan(new Query("temporal", ECQL.toFilter(f))).map(_.filter.index.name) mustEqual Seq(temporal.name)
      }
    }
  }
}

object GeoMesaDataStoreTest {

  class TestQueryInterceptor extends QueryInterceptor {
    override def init(ds: DataStore, sft: SimpleFeatureType): Unit = {}
    override def rewrite(query: Query): Unit =
      if (query.getFilter == Filter.INCLUDE) { query.setFilter(Filter.EXCLUDE) }
    override def close(): Unit = {}
  }

  class TestVisitor extends GeoMesaProcessVisitor {
    var executed = false
    var visited = false
    override def execute(source: SimpleFeatureSource, query: Query): Unit = executed = true
    override def visit(feature: Feature): Unit = visited = true
    override def getResult: CalcResult = null
  }

  // example class extending DecoratingFeatureCollection
  class TestFeatureCollection(delegate: SimpleFeatureCollection)
      extends DecoratingFeatureCollection[SimpleFeatureType, SimpleFeature](delegate)

  // example class extending DecoratingSimpleFeatureCollection
  class TestSimpleFeatureCollection(delegate: SimpleFeatureCollection)
      extends DecoratingSimpleFeatureCollection(delegate)
}
