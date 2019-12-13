/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import org.apache.accumulo.core.security.Authorizations
import org.geotools.data._
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithMultipleSfts
import org.locationtech.geomesa.accumulo.index.JoinIndex
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.index.attribute.AttributeIndex
import org.locationtech.geomesa.index.index.id.IdIndex
import org.locationtech.geomesa.index.index.z2.Z2Index
import org.locationtech.geomesa.index.index.z3.Z3Index
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.filter.Filter
import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class AccumuloDataStoreDeleteTest extends Specification with TestWithMultipleSfts {

  sequential

  lazy val tableOps = ds.connector.tableOperations()

  def createFeature(schema: String = "name:String:index=join,*geom:Point:srid=4326,dtg:Date") = {
    val sft = createNewSchema(schema, Some("dtg"))

    // create a feature
    val builder = new SimpleFeatureBuilder(sft)
    builder.addAll(List("1", WKTUtils.read("POINT(45.0 45.0)"), "2012-01-02T05:06:07.000Z"))
    val liveFeature = builder.buildFeature("fid-1")

    addFeature(sft, liveFeature)
    sft
  }

  "AccumuloDataStore" should {

    "delete a schema completely" in {
      val sft = createFeature()
      val typeName = sft.getTypeName

      // tests that tables exist before being deleted
      val indices = ds.manager.indices(sft)
      val tableNames = indices.flatMap(_.getTableNames())
      indices.map(_.name) must containTheSameElementsAs(Seq(JoinIndex, IdIndex, Z2Index, Z3Index).map(_.name))
      forall(tableNames)(tableOps.exists(_) must beTrue)

      // tests that metadata exists in the catalog before being deleted
      ds.getFeatureReader(new Query(typeName), Transaction.AUTO_COMMIT) must not(beNull)
      ds.metadata.getFeatureTypes.toSeq must contain(typeName)
      ds.stats.getCount(sft, exact = false) must beSome(1)

      // delete the schema
      ds.removeSchema(typeName)

      // tables should be deleted now (for stand-alone tables only)
      forall(tableNames)(tableOps.exists(_) must beFalse)

      // metadata should be deleted from the catalog now
      ds.metadata.getFeatureTypes.toSeq must not contain typeName
      ds.stats.getCount(sft, exact = false) must beNone

      ds.getFeatureSource(typeName).getFeatures(Filter.INCLUDE) must throwA[Exception]
    }

    "keep other tables when a separate schema is deleted" in {
      val sft1 = createFeature()
      val sft2 = createFeature()
      val typeName1 = sft1.getTypeName
      val typeName2 = sft2.getTypeName

      // tests that tables exist before being deleted
      val indices1 = ds.manager.indices(sft1)
      val tableNames1 = indices1.flatMap(_.getTableNames())
      indices1.map(_.name) must containTheSameElementsAs(Seq(JoinIndex, IdIndex, Z2Index, Z3Index).map(_.name))
      forall(tableNames1)(tableOps.exists(_) must beTrue)

      val indices2 = ds.manager.indices(sft2)
      val tableNames2 = indices2.flatMap(_.getTableNames())
      indices2.map(_.name) must containTheSameElementsAs(Seq(JoinIndex, IdIndex, Z2Index, Z3Index).map(_.name))
      forall(tableNames2)(tableOps.exists(_) must beTrue)

      def testExists(typeName: String): MatchResult[Option[Long]] = {
        ds.metadata.getFeatureTypes.toSeq must contain(typeName)
        val sft = ds.getSchema(typeName)
        sft must not(beNull)
        val reader = ds.getFeatureReader(new Query(typeName), Transaction.AUTO_COMMIT)
        try {
          reader.hasNext must beTrue
          reader must not(beNull)
        } finally {
          reader.close()
        }
        ds.stats.getCount(sft, exact = false) must beSome(1)
      }

      // tests that metadata exists in the catalog before being deleted
      forall(Seq(typeName1, typeName2))(testExists)

      ds.removeSchema(typeName1)

      // ensure second sft wasn't deleted
      testExists(typeName2)

      // ensure first sft was deleted
      forall(tableNames1)(tableOps.exists(_) must beFalse)
      ds.metadata.getFeatureTypes.toSeq must not contain typeName1
      ds.getSchema(typeName1) must beNull
      ds.stats.getCount(sft1, exact = false) must beNone
    }

    "delete non-point geometries" >> {
      val spec = "name:String:index=join,*geom:Geometry:srid=4326,dtg:Date;geomesa.mixed.geometries='true'"
      val sft1 = createFeature(spec)
      val sft2 = createFeature(spec)

      val feature1 = new ScalaSimpleFeature(sft1, "fid")
      val feature2 = new ScalaSimpleFeature(sft2, "fid")

      feature1.setAttribute(0, "name")
      feature1.setAttribute(1, "POLYGON((41 28, 42 28, 42 29, 41 29, 41 28))")
      feature1.setAttribute(2, "2015-01-01T00:30:00.000Z")
      feature2.setAttribute(0, "name")
      feature2.setAttribute(1, "POLYGON((41 28, 42 28, 42 29, 41 29, 41 28))")
      feature2.setAttribute(2, "2015-01-01T00:30:00.000Z")

      addFeatures(sft1, Seq(feature1))
      addFeatures(sft2, Seq(feature2))

      val typeNames = Seq(sft1.getTypeName, sft2.getTypeName)

      val filters = Seq("IN ('fid')", "name = 'name'", "bbox(geom, 40, 27, 43, 30)",
        "bbox(geom, 40, 27, 43, 30) AND dtg DURING 2015-01-01T00:00:00.000Z/2015-01-01T01:00:00.000Z")

      // verify that features come back
      forall(filters) { f =>
        val filter = ECQL.toFilter(f)
        forall(typeNames) { typeName =>
          val query = new Query(typeName, filter)
          val res = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).map(_.getID).toSeq
          res mustEqual Seq("fid")
        }
      }

      // remove the features
      typeNames.foreach { typeName =>
        val remover = ds.getFeatureWriter(typeName, ECQL.toFilter("IN ('fid')"), Transaction.AUTO_COMMIT)
        remover.next
        remover.remove()
        remover.close()
      }

      // verify that features no longer come back
      forall(filters) { f =>
        val filter = ECQL.toFilter(f)
        forall(typeNames) { typeName =>
          val query = new Query(typeName, filter)
          SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).map(_.getID).toSeq must beEmpty
        }
      }
    }

    "delete all associated tables" >> {
      val catalog = "AccumuloDataStoreTotalDeleteTest"
      val params = dsParams ++ Map(AccumuloDataStoreParams.CatalogParam.key -> catalog)
      val ds = DataStoreFinder.getDataStore(params).asInstanceOf[AccumuloDataStore]
      val sft = SimpleFeatureTypes.createType(catalog, "name:String:index=join,dtg:Date,*geom:Point:srid=4326")
      ds.createSchema(sft)
      val tables = ds.manager.indices(sft).flatMap(_.getTableNames()) ++ Seq(catalog, s"${catalog}_stats")
      tables must haveSize(6)
      forall(tables)(tableOps.exists(_) must beTrue)
      ds.delete()
      forall(tables)(tableOps.exists(_) must beFalse)
    }
  }
}
