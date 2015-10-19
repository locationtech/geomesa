/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.accumulo.data

import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.security.Authorizations
import org.geotools.data._
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithMultipleSfts
import org.locationtech.geomesa.accumulo.data.tables._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class AccumuloDataStoreDeleteTest extends Specification with TestWithMultipleSfts {

  sequential

  lazy val tableOps = ds.connector.tableOperations()

  def createFeature(tableSharing: Boolean, numShards: Option[Int] = None) = {
    val sft = createNewSchema("name:String:index=true,*geom:Point:srid=4326,dtg:Date", Some("dtg"), tableSharing, numShards)

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
      val tables = GeoMesaTable.getTables(sft)
      val tableNames = tables.map(ds.getTableName(typeName, _))
      tables must containTheSameElementsAs(Seq(AttributeTable, RecordTable, SpatioTemporalTable, Z3Table))
      forall(tableNames)(tableOps.exists(_) must beTrue)

      // tests that metadata exists in the catalog before being deleted
      ds.getFeatureReader(typeName) must not(beNull)
      ds.metadata.getFeatureTypes.toSeq must contain(typeName)

      // delete the schema
      ds.removeSchema(typeName)

      // tables should be deleted now (for stand-alone tables only)
      forall(tableNames)(tableOps.exists(_) must beFalse)

      // metadata should be deleted from the catalog now
      ds.metadata.getFeatureTypes.toSeq must not contain typeName

      ds.getFeatureSource(typeName).getFeatures(Filter.INCLUDE) must throwA[Exception]
    }

    "keep other tables when a separate schema is deleted" in {
      val sft1 = createFeature(tableSharing = false)
      val sft2 = createFeature(tableSharing = false)
      val typeName1 = sft1.getTypeName
      val typeName2 = sft2.getTypeName

      // tests that tables exist before being deleted
      val tables1 = GeoMesaTable.getTables(sft1)
      val tableNames1 = tables1.map(ds.getTableName(typeName1, _))
      tables1 must containTheSameElementsAs(Seq(AttributeTable, RecordTable, SpatioTemporalTable, Z3Table))
      forall(tableNames1)(tableOps.exists(_) must beTrue)

      val tables2 = GeoMesaTable.getTables(sft2)
      val tableNames2 = tables2.map(ds.getTableName(typeName2, _))
      tables2 must containTheSameElementsAs(Seq(AttributeTable, RecordTable, SpatioTemporalTable, Z3Table))
      forall(tableNames2)(tableOps.exists(_) must beTrue)

      // tests that metadata exists in the catalog before being deleted
      ds.getFeatureReader(typeName1) should not(beNull)
      ds.getFeatureReader(typeName2) should not(beNull)
      ds.metadata.getFeatureTypes.toSeq must contain(typeName1)
      ds.metadata.getFeatureTypes.toSeq must contain(typeName2)

      ds.removeSchema(typeName1)

      // these tables should be deleted now
      forall(tableNames1)(tableOps.exists(_) must beFalse)
      // but these tables should still exist since sftName2 wasn't deleted
      forall(tableNames2)(tableOps.exists(_) must beTrue)

      // metadata should be deleted from the catalog now for sftName
      ds.metadata.getFeatureTypes.toSeq must not contain typeName1
      // metadata should still exist for sftName2
      ds.metadata.getFeatureTypes.toSeq must contain(typeName2)

      ds.getFeatureSource(typeName2).getFeatures(Filter.INCLUDE).size() mustEqual 1
    }

    "delete schema and the z3 table on a shared table" in {
      val sft = createFeature(tableSharing = true)
      val sft2 = createFeature(tableSharing = true)
      val typeName = sft.getTypeName

      // tests that tables exist before being deleted
      val tables = GeoMesaTable.getTables(sft)
      val tableNames = tables.map(ds.getTableName(typeName, _))
      tables must containTheSameElementsAs(Seq(AttributeTable, RecordTable, SpatioTemporalTable, Z3Table))
      forall(tableNames)(tableOps.exists(_) must beTrue)

      ds.metadata.getFeatureTypes.toSeq must contain(typeName)

      ds.removeSchema(typeName)

      // z3 table must be gone
      forall(tableNames.filterNot(_.contains("z3")))(tableOps.exists(_) must beTrue)
      forall(tableNames.filter(_.contains("z3")))(tableOps.exists(_) must beFalse)

      // metadata should be deleted from the catalog now
      ds.metadata.getFeatureTypes.toSeq must not contain typeName

      ds.getFeatureSource(typeName).getFeatures(Filter.INCLUDE) must throwA[Exception]
    }

    "delete entries on a shared table" in {
      val sft1 = createFeature(tableSharing = true, Some(10))
      val sft2 = createFeature(tableSharing = true, Some(10))

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
      val tables1 = GeoMesaTable.getTables(sft1)
      val tableNames1 = tables1.map(ds.getTableName(typeName1, _))
      tables1 must containTheSameElementsAs(Seq(AttributeTable, RecordTable, SpatioTemporalTable, Z3Table))
      forall(tableNames1)(tableOps.exists(_) must beTrue)

      val tables2 = GeoMesaTable.getTables(sft2)
      val tableNames2 = tables2.map(ds.getTableName(typeName2, _))
      tables2 must containTheSameElementsAs(Seq(AttributeTable, RecordTable, SpatioTemporalTable, Z3Table))
      forall(tableNames2)(tableOps.exists(_) must beTrue)

      // tests that metadata exists in the catalog before being deleted
      ds.getFeatureReader(typeName1) should not(beNull)
      ds.getFeatureReader(typeName2) should not(beNull)
      ds.metadata.getFeatureTypes.toSeq must contain(typeName1)
      ds.metadata.getFeatureTypes.toSeq must contain(typeName2)

      ds.removeSchema(typeName1)

      // shared tables should still exist
      forall(tableNames1.filterNot(_.contains("z3")))(tableOps.exists(_) must beTrue)
      forall(tableNames1.filter(_.contains("z3")))(tableOps.exists(_) must beFalse)
      // but these tables should still exist since sftName2 wasn't deleted
      forall(tableNames2)(tableOps.exists(_) must beTrue)

      // metadata should be deleted from the catalog now for sftName
      ds.metadata.getFeatureTypes.toSeq must not contain typeName1
      // metadata should still exist for sftName2
      ds.metadata.getFeatureTypes.toSeq must contain(typeName2)

      // ensure st entries are deleted
      val stTableOpt = tableNames2.find(_.contains("st_idx"))
      stTableOpt must beSome
      val stTable = stTableOpt.get
      ds.connector.createScanner(stTable, new Authorizations()).toSeq must haveSize(202)
    }

    "delete all associated tables" >> {
      val catalog = "AccumuloDataStoreTotalDeleteTest"
      val connector = new MockInstance("mycloud").getConnector("user", new PasswordToken("password"))
      val ds = DataStoreFinder.getDataStore(Map("connector" -> connector, "tableName" -> catalog)).asInstanceOf[AccumuloDataStore]
      val sft = SimpleFeatureTypes.createType(catalog, "name:String:index=true,dtg:Date,*geom:Point:srid=4326")
      ds.createSchema(sft)
      val tables = GeoMesaTable.getTableNames(sft, ds) ++ Seq(catalog)
      tables must haveSize(5)
      forall(tables)(tableOps.exists(_) must beTrue)
      ds.delete()
      forall(tables)(tableOps.exists(_) must beFalse)
    }
  }
}
