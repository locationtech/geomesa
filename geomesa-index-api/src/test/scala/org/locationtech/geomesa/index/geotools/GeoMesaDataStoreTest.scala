/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.geotools

import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.{DataStore, Query, Transaction}
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.{JTS, ReferencedEnvelope}
import org.geotools.referencing.CRS
import org.geotools.util.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.TestGeoMesaDataStore
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreTest._
import org.locationtech.geomesa.index.index.attribute.AttributeIndex
import org.locationtech.geomesa.index.index.id.IdIndex
import org.locationtech.geomesa.index.index.z3.Z3Index
import org.locationtech.geomesa.index.planning.QueryInterceptor
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.jts.geom.{Geometry, Point}
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
    "respect max features" in {
      val query = new Query("test")
      query.setMaxFeatures(1)
      ds.getFeatureSource(sft.getTypeName).getCount(query) mustEqual 1
      ok
    }

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
    "block queries which would cause a full table scan" in {
      val sft = SimpleFeatureTypes.createType("61b44359ddb84822983587389d6a28a4",
        "name:String,age:Int,dtg:Date,*geom:Point:srid=4326;geomesa.indices.enabled='id,z3,attr:name'")
      sft.getUserData.put("geomesa.query.interceptors",
        "org.locationtech.geomesa.index.planning.guard.FullTableScanQueryGuard");

      val ds = new TestGeoMesaDataStore(true)
      ds.createSchema(sft)

      val valid = Seq(
        "name = 'bob'",
        "IN('123')",
        "bbox(geom,-10,-10,10,10) AND dtg during 2020-01-01T00:00:00.000Z/2020-01-01T23:59:59.000Z",
        "bbox(geom,-10,-10,10,10) AND (dtg during 2020-01-01T00:00:00.000Z/2020-01-01T00:59:59.000Z OR dtg during 2020-01-01T12:00:00.000Z/2020-01-01T12:59:59.000Z)"
      )

      val invalid = Seq(
        "INCLUDE",
        "bbox(geom,-180,-90,180,90)",
        "name ilike '%b'",
        "not IN('1')"
      )

      foreach(valid.map(ECQL.toFilter)) { filter =>
        SelfClosingIterator(ds.getFeatureReader(new Query(sft.getTypeName, filter), Transaction.AUTO_COMMIT)).toList must
          beEmpty
      }

      foreach(invalid.map(ECQL.toFilter)) { filter =>
        val query = new Query(sft.getTypeName, filter)
        SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList must
            throwAn[IllegalArgumentException]
        // you can set max features and use a full-table scan
        query.setMaxFeatures(50)
        SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList must beEmpty
      }
      ds.dispose()

      // create a new store so the sys prop gets evaluated when the query guards are loaded
      val ds2 = new TestGeoMesaDataStore(true)
      System.setProperty(s"geomesa.scan.${sft.getTypeName}.block-full-table", "false")
      try {
        ds2.createSchema(sft)
        foreach(invalid.map(ECQL.toFilter)) { filter =>
          SelfClosingIterator(ds2.getFeatureReader(new Query(sft.getTypeName, filter), Transaction.AUTO_COMMIT)).toList must
              beEmpty
        }
      } finally {
        System.clearProperty(s"geomesa.scan.${sft.getTypeName}.block-full-table")
        ds2.dispose()
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
    "check provided fid hints during modifying writes" in {
      val spec = "name:String:index=true,age:Int,dtg:Date,*geom:Point:srid=4326"
      val ds = new TestGeoMesaDataStore(true)
      ds.createSchema(SimpleFeatureTypes.createType("test", spec))
      val feature = ScalaSimpleFeature.create(sft, "0", "name", "20", "2020-01-20T00:00:00.000Z", "POINT (45 55)")
      WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
        FeatureUtils.write(writer, feature, useProvidedFid = true)
      }
      SelfClosingIterator(ds.getFeatureReader(new Query(sft.getTypeName), Transaction.AUTO_COMMIT)).toList mustEqual
          Seq(feature)
      WithClose(ds.getFeatureWriter(sft.getTypeName, ECQL.toFilter("IN ('0')"), Transaction.AUTO_COMMIT)) { writer =>
        writer.hasNext must beTrue
        val update = writer.next
        update.getUserData.put(Hints.PROVIDED_FID, "1")
        writer.write()
      }
      val newId = ScalaSimpleFeature.copy(feature)
      newId.setId("1")
      SelfClosingIterator(ds.getFeatureReader(new Query(sft.getTypeName), Transaction.AUTO_COMMIT)).toList mustEqual
          Seq(newId)
    }
    "checkout duplicate attribute names" in {
      val spec = "foo:String,bar:Int,foo:Double,*geom:Point:srid=4326"
      val sft = SimpleFeatureTypes.createType("test",spec)
      val ds = new TestGeoMesaDataStore(true)
      ds.createSchema(sft) must throwAn[IllegalArgumentException]
    }
    "handle literal geometries in dwithin filters" in {
      val spec = "name:String,dtg:Date,*geom:Point:srid=4326"
      val sft = SimpleFeatureTypes.createType("test",spec)
      val ds = new TestGeoMesaDataStore(true)
      ds.createSchema(sft)
      val feature = ScalaSimpleFeature.create(sft, "0", "name", "2020-01-20T00:00:00.000Z", "POINT (45 55)")
      WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
        FeatureUtils.write(writer, feature, useProvidedFid = true)
      }
      val filters =
        Seq(
          "dwithin(geom,POINT(45.01 55.01),10000,meters)",
          "dwithin(geom,'POINT(45.01 55.01)',10000,meters)"
        )
      foreach(filters.map(ECQL.toFilter)) { filter =>
        val query = new Query(sft.getTypeName, filter)
        SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList mustEqual Seq(feature)
      }
    }
    "handle ORs between indexed attribute fields" >> {
      val spec =
        "attr1:Long:cardinality=high:index=true,attr2:Long:cardinality=high:index=true," +
            "name:String:cardinality=high:index=true,dtg:Date,*geom:Point:srid=4326"
      val ds = new TestGeoMesaDataStore(true)
      ds.createSchema(SimpleFeatureTypes.createType("test", spec))
      val filter =
        ECQL.toFilter("(attr1=1 OR attr2=2 OR name='3') and dtg during 2020-01-01T00:00:00.000Z/2020-01-02T00:00:00.000Z")
      val plans = ds.getQueryPlan(new Query("test", filter))
      plans must haveLength(3)
      foreach(plans)(_.filter.index.name mustEqual AttributeIndex.name)
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
}
