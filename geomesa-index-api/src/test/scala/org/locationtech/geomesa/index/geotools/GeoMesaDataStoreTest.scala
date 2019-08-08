/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
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
import org.geotools.factory.Hints
import org.geotools.feature.collection.{DecoratingFeatureCollection, DecoratingSimpleFeatureCollection}
import org.geotools.feature.visitor.CalcResult
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.JTS
import org.geotools.referencing.CRS
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.TestGeoMesaDataStore
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreTest.{TestFeatureCollection, TestQueryInterceptor, TestSimpleFeatureCollection, TestVisitor}
import org.locationtech.geomesa.index.planning.QueryInterceptor
import org.locationtech.geomesa.index.process.GeoMesaProcessVisitor
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
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
    "intercept and rewrite queries" in {
      val sft = SimpleFeatureTypes.createType("rewrite", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
      sft.getUserData.put(SimpleFeatureTypes.Configs.QUERY_INTERCEPTORS, classOf[TestQueryInterceptor].getName)

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
