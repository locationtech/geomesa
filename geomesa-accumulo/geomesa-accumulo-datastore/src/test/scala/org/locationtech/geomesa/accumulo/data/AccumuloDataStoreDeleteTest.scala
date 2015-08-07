/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.accumulo.data

import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.data.{Mutation, Range}
import org.apache.accumulo.core.iterators.user.VersioningIterator
import org.apache.commons.codec.binary.Hex
import org.geotools.data._
import org.geotools.factory.Hints
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.accumulo.data.tables.GeoMesaTable
import org.locationtech.geomesa.accumulo.util.GeoMesaBatchWriterConfig
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class AccumuloDataStoreDeleteTest extends Specification with TestWithDataStore {

  sequential

  val spec = "name:String,dtg:Date,*geom:Point:srid=4326"

  "AccumuloDataStore" should {
    "support deleting schemas" in {

      def scanMetadata(ds: AccumuloDataStore, sftName: String): Option[String] = {
        val scanner = ds.connector.createScanner(ds.catalogTable, ds.authorizationsProvider.getAuthorizations)
        scanner.setRange(new Range(s"${METADATA_TAG }_$sftName"))

        val name = "version-" + sftName
        val cfg = new IteratorSetting(1, name, classOf[VersioningIterator])
        VersioningIterator.setMaxVersions(cfg, 1)
        scanner.addScanIterator(cfg)

        val iter = scanner.iterator
        val result =
          if (iter.hasNext) {
            Some(iter.next.getValue.toString)
          } else {
            None
          }

        scanner.close()
        scanner.removeScanIterator(name)
        result
      }

      def buildPreSecondaryIndexTable(params: Map[String, String], sftName: String) = {
        val rowIds = List(
          "09~regressionTestType~v00~20120102",
          "95~regressionTestType~v00~20120102",
          "53~regressionTestType~v00~20120102",
          "77~regressionTestType~v00~20120102",
          "36~regressionTestType~v00~20120102",
          "91~regressionTestType~v00~20120102")
        val hex = new Hex
        val indexValues = List(
          "000000013000000015000000000140468000000000004046800000000000000001349ccf6e18",
          "000000013100000015000000000140468000000000004046800000000000000001349ccf6e18",
          "000000013200000015000000000140468000000000004046800000000000000001349ccf6e18",
          "000000013300000015000000000140468000000000004046800000000000000001349ccf6e18",
          "000000013400000015000000000140468000000000004046800000000000000001349ccf6e18",
          "000000013500000015000000000140468000000000004046800000000000000001349ccf6e18").map {v =>
          hex.decode(v.getBytes)}
        val sft = SimpleFeatureTypes.createType(sftName, "name:String,geom:Geometry:srid=4326,dtg:Date,dtg_end_time:Date")
        sft.setDtgField("dtg")

        val instance = new MockInstance(params("instanceId"))
        val connector = instance.getConnector(params("user"), new PasswordToken(params("password").getBytes))
        connector.tableOperations.create(params("tableName"))

        val bw = connector.createBatchWriter(params("tableName"), GeoMesaBatchWriterConfig())

        // Insert metadata
        val metadataMutation = new Mutation(s"~METADATA_$sftName")
        metadataMutation.put("attributes", "", "name:String,geom:Geometry:srid=4326,dtg:Date,dtg_end_time:Date")
        metadataMutation.put("bounds", "", "45.0:45.0:49.0:49.0")
        metadataMutation.put("schema", "", s"%~#s%99#r%$sftName#cstr%0,3#gh%yyyyMMdd#d::%~#s%3,2#gh::%~#s%#id")
        bw.addMutation(metadataMutation)

        // Insert features
        createTestFeatures(sft).zipWithIndex.foreach { case(sf, idx) =>
          val encoded = DataUtilities.encodeFeature(sf)
          val index = new Mutation(rowIds(idx))
          index.put("00".getBytes,sf.getID.getBytes, indexValues(idx))
          bw.addMutation(index)

          val data = new Mutation(rowIds(idx))
          data.put(sf.getID, "SimpleFeatureAttribute", encoded)
          bw.addMutation(data)
        }

        bw.flush()
        bw.close()
      }

      def createFeature(sftName: String, ds: AccumuloDataStore, sharedTables: Boolean = true) = {
        val sft = SimpleFeatureTypes.createType(sftName, "name:String:index=true,geom:Point:srid=4326,dtg:Date")
        sft.setTableSharing(sharedTables)
        ds.createSchema(sft)
        val fs = ds.getFeatureSource(sft.getTypeName).asInstanceOf[AccumuloFeatureStore]

        // create a feature
        val builder = new SimpleFeatureBuilder(sft)
        builder.addAll(List("1", WKTUtils.read("POINT(45.0 45.0)"), "2012-01-02T05:06:07.000Z"))
        val liveFeature = builder.buildFeature("fid-1")

        // make sure we ask the system to re-use the provided feature-ID
        liveFeature.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        val featureCollection = new DefaultFeatureCollection(sft.getTypeName, sft)
        featureCollection.add(liveFeature)
        fs.addFeatures(featureCollection)
      }

      "delete the schema completely" in {
        val sftName = "deleteSchemaTest"
        val table = "testing_delete_schema"
        val ds = DataStoreFinder.getDataStore(Map(
          "instanceId"        -> "mycloud",
          "zookeepers"        -> "zoo1:2181,zoo2:2181,zoo3:2181",
          "user"              -> "myuser",
          "password"          -> "mypassword",
          "tableName"         -> table,
          "useMock"           -> "true")).asInstanceOf[AccumuloDataStore]

        ds must not beNull

        createFeature(sftName, ds, sharedTables = false)

        val c = ds.connector

        // tests that tables exist before being deleted
        c.tableOperations().exists(s"${table}_${sftName}_st_idx") must beTrue
        c.tableOperations().exists(s"${table}_${sftName}_records") must beTrue
        c.tableOperations().exists(s"${table}_${sftName}_attr_idx") must beTrue

        val fr = ds.getFeatureReader(sftName)
        // tests that metadata exists in the catalog before being deleted
        fr must not beNull

        scanMetadata(ds, sftName) should beSome

        ds.removeSchema(sftName)

        //tables should be deleted now (for stand-alone tables only)
        c.tableOperations().exists(s"${table}_${sftName}_st_idx") must beFalse
        c.tableOperations().exists(s"${table}_${sftName}_records") must beFalse
        c.tableOperations().exists(s"${table}_${sftName}_attr_idx") must beFalse

        //metadata should be deleted from the catalog now
        scanMetadata(ds, sftName) should beNone

        val query = new Query(sftName, Filter.INCLUDE)
        ds.getFeatureSource(sftName).getFeatures(query) must throwA[Exception]
      }

      "throw a RuntimeException when calling removeSchema on 0.10.x records" in {
        val sftName = "regressionRemoveSchemaTest"

        val manualParams = Map(
          "instanceId" -> "mycloud",
          "zookeepers" -> "zoo1:2181,zoo2:2181,zoo3:2181",
          "user"       -> "myuser",
          "password"   -> "mypassword",
          "auths"      -> "A,B,C",
          "useMock"    -> "true",
          "tableName"  -> "manualTableForDeletion")

        buildPreSecondaryIndexTable(manualParams, sftName)

        val manualStore = DataStoreFinder.getDataStore(manualParams).asInstanceOf[AccumuloDataStore]
        manualStore.removeSchema(sftName) should throwA[RuntimeException]
      }

      "keep other tables when a separate schema is deleted" in {
        val sftName = "deleteSharedSchemaTest"
        val sftName2 = "deleteSharedSchemaTest2"

        val table = "testing_shared_delete_schema"
        val ds = DataStoreFinder.getDataStore(Map(
          "instanceId"        -> "mycloud",
          "zookeepers"        -> "zoo1:2181,zoo2:2181,zoo3:2181",
          "user"              -> "myuser",
          "password"          -> "mypassword",
          "tableName"         -> table,
          "useMock"           -> "true")).asInstanceOf[AccumuloDataStore]

        ds should not beNull

        createFeature(sftName, ds, sharedTables = false)
        createFeature(sftName2, ds, sharedTables = false)

        val c = ds.connector

        //tests that tables exist before being deleted
        c.tableOperations().exists(s"${table}_${sftName}_st_idx") must beTrue
        c.tableOperations().exists(s"${table}_${sftName}_records") must beTrue
        c.tableOperations().exists(s"${table}_${sftName}_attr_idx") must beTrue
        c.tableOperations().exists(s"${table}_${sftName2}_st_idx") must beTrue
        c.tableOperations().exists(s"${table}_${sftName2}_records") must beTrue
        c.tableOperations().exists(s"${table}_${sftName2}_attr_idx") must beTrue

        val fr = ds.getFeatureReader(sftName)
        val fr2 = ds.getFeatureReader(sftName2)
        //tests that metadata exists in the catalog before being deleted
        fr should not be null
        fr2 should not be null

        val scannerResults = scanMetadata(ds, sftName)
        val scannerResults2 = scanMetadata(ds, sftName2)
        scannerResults should beSome
        scannerResults2 should beSome

        ds.removeSchema(sftName)

        //these tables should be deleted now
        c.tableOperations().exists(s"${table}_${sftName}_st_idx") must beFalse
        c.tableOperations().exists(s"${table}_${sftName}_records") must beFalse
        c.tableOperations().exists(s"${table}_${sftName}_attr_idx") must beFalse
        //but these tables should still exist since sftName2 wasn't deleted
        c.tableOperations().exists(s"${table}_${sftName2}_st_idx") must beTrue
        c.tableOperations().exists(s"${table}_${sftName2}_records") must beTrue
        c.tableOperations().exists(s"${table}_${sftName2}_attr_idx") must beTrue

        val scannerResultsAfterDeletion = scanMetadata(ds, sftName)
        val scannerResultsAfterDeletion2 = scanMetadata(ds, sftName2)

        //metadata should be deleted from the catalog now for sftName
        scannerResultsAfterDeletion should beNone
        //metadata should still exist for sftName2
        scannerResultsAfterDeletion2 should beSome

        val query2 = new Query(sftName2, Filter.INCLUDE)

        val results2 = ds.getFeatureSource(sftName2).getFeatures(query2)
        results2.size() should beGreaterThan(0)
      }

      "delete schema and the z3 table on a shared table" in {
        val sftName = "deleteSchemaOnSharedTableTest"
        val table = "testing_delete_schema"
        val ds = DataStoreFinder.getDataStore(Map(
          "instanceId"        -> "mycloud",
          "zookeepers"        -> "zoo1:2181,zoo2:2181,zoo3:2181",
          "user"              -> "myuser",
          "password"          -> "mypassword",
          "tableName"         -> table,
          "useMock"           -> "true")).asInstanceOf[AccumuloDataStore]

        ds must not beNull

        createFeature(sftName, ds, sharedTables = true)

        val c = ds.connector

        // tests that tables exist before being deleted
        c.tableOperations().exists(s"${table}") must beTrue
        c.tableOperations().exists(s"${table}_st_idx") must beTrue
        c.tableOperations().exists(s"${table}_records") must beTrue
        c.tableOperations().exists(s"${table}_attr_idx") must beTrue
        c.tableOperations().exists(s"${table}_${sftName}_z3") must beTrue

        val fr = ds.getFeatureReader(sftName)
        // tests that metadata exists in the catalog before being deleted
        fr must not beNull

        scanMetadata(ds, sftName) should beSome

        ds.removeSchema(sftName)

        //z3 table must be gone
        c.tableOperations().exists(s"${table}") must beTrue
        c.tableOperations().exists(s"${table}_st_idx") must beTrue
        c.tableOperations().exists(s"${table}_records") must beTrue
        c.tableOperations().exists(s"${table}_attr_idx") must beTrue
        c.tableOperations().exists(s"${table}_${sftName}_z3") must beFalse

        //metadata should be deleted from the catalog now
        scanMetadata(ds, sftName) should beNone

        val query = new Query(sftName, Filter.INCLUDE)
        ds.getFeatureSource(sftName).getFeatures(query) must throwA[Exception]
      }

    }

    "delete all associated tables" >> {
      val catalog = "AccumuloDataStoreDeleteTest"
      val connector = new MockInstance("mycloud").getConnector("user", new PasswordToken("password"))
      val ds = DataStoreFinder.getDataStore(Map(
        "connector" -> connector,
        // note the table needs to be different to prevent testing errors
        "tableName" -> catalog)).asInstanceOf[AccumuloDataStore]
      val sft = SimpleFeatureTypes.createType(catalog, "name:String:index=true,dtg:Date,*geom:Point:srid=4326")
      ds.createSchema(sft)
      val tables = GeoMesaTable.getTableNames(sft, ds) ++ Seq(catalog)
      tables must haveSize(5)
      connector.tableOperations().list().toSeq must containAllOf(tables)
      ds.delete()
      connector.tableOperations().list().toSeq must not(containAnyOf(tables))
    }
  }

  /**
   * Creates 6 features.
   *
   * @param sft
   */
  def createTestFeatures(sft: SimpleFeatureType) = (0 until 6).map { i =>
    val builder = new SimpleFeatureBuilder(sft)
    builder.set("geom", WKTUtils.read("POINT(45.0 45.0)"))
    builder.set("dtg", "2012-01-02T05:06:07.000Z")
    builder.set("name",i.toString)
    val sf = builder.buildFeature(i.toString)
    sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
    sf
  }
}
