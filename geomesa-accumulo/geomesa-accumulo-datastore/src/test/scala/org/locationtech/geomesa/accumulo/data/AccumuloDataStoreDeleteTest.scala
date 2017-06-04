/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.security.Authorizations
import org.geotools.data._
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithMultipleSfts
import org.locationtech.geomesa.accumulo.index._
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.index.IndexMode
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

  def createFeature(tableSharing: Boolean,
                    schema: String = "name:String:index=true,*geom:Point:srid=4326,dtg:Date") = {
    val sft = createNewSchema(schema, Some("dtg"), tableSharing)

    // create a feature
    val builder = new SimpleFeatureBuilder(sft)
    builder.addAll(List("1", WKTUtils.read("POINT(45.0 45.0)"), "2012-01-02T05:06:07.000Z"))
    val liveFeature = builder.buildFeature("fid-1")

    addFeature(sft, liveFeature)
    sft
  }

  "AccumuloDataStore" should {

    "delete a schema completely" in {
      val sft = createFeature(tableSharing = false)
      val typeName = sft.getTypeName

      // tests that tables exist before being deleted
      val tables = AccumuloFeatureIndex.indices(sft, IndexMode.Any)
      val tableNames = tables.map(_.getTableName(typeName, ds))
      tables must containTheSameElementsAs(Seq(AttributeIndex, RecordIndex, Z2Index, Z3Index))
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
      val sft1 = createFeature(tableSharing = false)
      val sft2 = createFeature(tableSharing = false)
      val typeName1 = sft1.getTypeName
      val typeName2 = sft2.getTypeName

      // tests that tables exist before being deleted
      val tables1 = AccumuloFeatureIndex.indices(sft1, IndexMode.Any)
      val tableNames1 = tables1.map(_.getTableName(typeName1, ds))
      tables1 must containTheSameElementsAs(Seq(AttributeIndex, RecordIndex, Z2Index, Z3Index))
      forall(tableNames1)(tableOps.exists(_) must beTrue)

      val tables2 = AccumuloFeatureIndex.indices(sft2, IndexMode.Any)
      val tableNames2 = tables2.map(_.getTableName(typeName2, ds))
      tables2 must containTheSameElementsAs(Seq(AttributeIndex, RecordIndex, Z2Index, Z3Index))
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

    "delete schema and the z3 table on a shared table" in {
      val sft = createFeature(tableSharing = true)
      val sft2 = createFeature(tableSharing = true)
      val typeName = sft.getTypeName

      // tests that tables exist before being deleted
      val tables = AccumuloFeatureIndex.indices(sft, IndexMode.Any)
      val tableNames = tables.map(_.getTableName(typeName, ds))
      tables must containTheSameElementsAs(Seq(AttributeIndex, RecordIndex, Z2Index, Z3Index))
      forall(tableNames)(tableOps.exists(_) must beTrue)

      ds.metadata.getFeatureTypes.toSeq must contain(typeName)

      ds.removeSchema(typeName)

      forall(tableNames)(tableOps.exists(_) must beTrue)

      // metadata should be deleted from the catalog now
      ds.metadata.getFeatureTypes.toSeq must not contain typeName

      ds.getFeatureSource(typeName).getFeatures(Filter.INCLUDE) must throwA[Exception]
    }

    "delete entries on a shared table" in {
      val sft1 = createFeature(tableSharing = true)
      val sft2 = createFeature(tableSharing = true)

      val builder1 = new SimpleFeatureBuilder(sft1)
      val builder2 = new SimpleFeatureBuilder(sft2)
      val features = (0 until 100).map { i =>
        val values = List(s"$i", WKTUtils.read("POINT(45.0 45.0)"), "2012-01-02T05:06:07.000Z")
        builder1.addAll(values)
        builder2.addAll(values)
        (builder1.buildFeature(s"fid-$i"), builder2.buildFeature(s"fid-$i"))
      }

      addFeatures(sft1, features.map(_._1))
      addFeatures(sft2, features.map(_._2))

      val typeName1 = sft1.getTypeName
      val typeName2 = sft2.getTypeName

      // tests that tables exist before being deleted
      val tables1 = AccumuloFeatureIndex.indices(sft1, IndexMode.Any)
      val tableNames1 = tables1.map(_.getTableName(typeName1, ds))
      tables1 must containTheSameElementsAs(Seq(AttributeIndex, RecordIndex, Z2Index, Z3Index))
      forall(tableNames1)(tableOps.exists(_) must beTrue)

      val tables2 = AccumuloFeatureIndex.indices(sft2, IndexMode.Any)
      val tableNames2 = tables2.map(_.getTableName(typeName2, ds))
      tables2 must containTheSameElementsAs(Seq(AttributeIndex, RecordIndex, Z2Index, Z3Index))
      forall(tableNames2)(tableOps.exists(_) must beTrue)

      // tests that metadata exists in the catalog before being deleted
      ds.getFeatureReader(new Query(typeName1), Transaction.AUTO_COMMIT) should not(beNull)
      ds.getFeatureReader(new Query(typeName2), Transaction.AUTO_COMMIT) should not(beNull)
      ds.metadata.getFeatureTypes.toSeq must contain(typeName1)
      ds.metadata.getFeatureTypes.toSeq must contain(typeName2)

      ds.removeSchema(typeName1)

      // shared tables should still exist
      forall(tableNames1)(tableOps.exists(_) must beTrue)
      // but these tables should still exist since sftName2 wasn't deleted
      forall(tableNames2)(tableOps.exists(_) must beTrue)

      // metadata should be deleted from the catalog now for sftName
      ds.metadata.getFeatureTypes.toSeq must not contain typeName1
      // metadata should still exist for sftName2
      ds.metadata.getFeatureTypes.toSeq must contain(typeName2)

      // ensure z2 entries are deleted
      val z2TableOpt = tableNames2.find(_.contains("z2"))
      z2TableOpt must beSome
      val z2Table = z2TableOpt.get
      ds.connector.createScanner(z2Table, new Authorizations()).toSeq must haveSize(101)
    }

    "delete non-point geometries" >> {
      val spec = "name:String:index=true,*geom:Geometry:srid=4326,dtg:Date;geomesa.mixed.geometries='true'"
      val sft1 = createFeature(tableSharing = true, spec)
      val sft2 = createFeature(tableSharing = false, spec)

      val feature1 = new ScalaSimpleFeature("fid", sft1)
      val feature2 = new ScalaSimpleFeature("fid", sft2)

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
        remover.remove
        remover.close
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
      val connector = new MockInstance("mycloud").getConnector("user", new PasswordToken("password"))
      val ds = DataStoreFinder.getDataStore(Map("connector" -> connector, "tableName" -> catalog)).asInstanceOf[AccumuloDataStore]
      val sft = SimpleFeatureTypes.createType(catalog, "name:String:index=true,dtg:Date,*geom:Point:srid=4326")
      ds.createSchema(sft)
      val tables = AccumuloFeatureIndex.indices(sft, IndexMode.Any).map(_.getTableName(sft.getTypeName, ds)) ++ Seq(catalog, s"${catalog}_stats")
      tables must haveSize(6)
      forall(tables)(tableOps.exists(_) must beTrue)
      ds.delete()
      forall(tables)(tableOps.exists(_) must beFalse)
    }
  }
}
