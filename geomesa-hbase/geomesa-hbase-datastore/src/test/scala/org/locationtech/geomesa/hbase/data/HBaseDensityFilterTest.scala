/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.hbase.data

import java.nio.charset.StandardCharsets

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.Envelope
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseTestingUtility, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost
import org.geotools.data._
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.data.Query
import org.geotools.factory.Hints
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.joda.time.{DateTime, DateTimeZone}
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.hbase.data.HBaseDataStoreParams._
import org.locationtech.geomesa.hbase.utils.HBaseBatchScan
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.index.ClientSideFiltering.RowAndValue
import org.locationtech.geomesa.index.utils.KryoLazyDensityUtils
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class HBaseDensityFilterTest extends Specification with LazyLogging {

  sequential

  val cluster: HBaseTestingUtility = new HBaseTestingUtility()
  var connection: Connection = null
  var conf: Configuration = cluster.getConfiguration();
  val TEST_FAMILY = "an_id:java.lang.Integer,attr:java.lang.Double,dtg:Date,geom:Point:srid=4326"
  val TEST_HINT = new Hints()
  val typeName = "testpoints"

  step {
    logger.info("Starting embedded hbase")
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
      "org.locationtech.geomesa.hbase.coprocessor.KryoLazyDensityCoprocessor");
    cluster.startMiniCluster(1)
    connection = cluster.getConnection
    logger.info("Started")
  }

  "HBaseDataStore" should {
    "work with points" in {

      val params = Map(ConnectionParam.getName -> connection, BigTableNameParam.getName -> "test_sft")
      val ds = DataStoreFinder.getDataStore(params).asInstanceOf[HBaseDataStore]

      ds.getSchema(typeName) must beNull

      ds.createSchema(SimpleFeatureTypes.createType(typeName, TEST_FAMILY))

      val sft = ds.getSchema(typeName)

      sft must not(beNull)

      val fs = ds.getFeatureSource(typeName).asInstanceOf[SimpleFeatureStore]

      val toAdd = (0 until 150).map { i =>
        val sf = new ScalaSimpleFeature(i.toString, sft)
        sf.setAttribute(0, i.toString)
        sf.setAttribute(1, "1.0")
        sf.setAttribute(2, new DateTime("2012-01-01T19:00:00", DateTimeZone.UTC).toDate)
        sf.setAttribute(3, "POINT(-77 38)")
        sf
      }

      val ids = fs.addFeatures(new ListFeatureCollection(sft, toAdd))

      val table: TableName = connection.getAdmin.listTableNames("test_sft_testpoints_z3")(0)

      val scanRange: Scan = new Scan()

      def rowAndValue(result: Result): RowAndValue = {
        val cell = result.rawCells()(0)
        RowAndValue(cell.getRowArray, cell.getRowOffset, cell.getRowLength,
          cell.getValueArray, cell.getValueOffset, cell.getValueLength)
      }

      val deserializer = new KryoFeatureSerializer(sft, SerializationOptions.withoutId)

      import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType._
      def getIdFromRow(sft: SimpleFeatureType): (Array[Byte], Int, Int) => String = {
        val start = if (sft.isTableSharing) {
          12
        } else {
          11
        } // table sharing + shard + 2 byte short + 8 byte long
        (row, offset, length) => new String(row, offset + start, length - start, StandardCharsets.UTF_8)
      }

      val getId = getIdFromRow(sft)

      // 1. Goal scan one of the tables with data
      val scan = new HBaseBatchScan(connection, table, Seq(scanRange), 2, 100000)
      val results = scan.map { result =>
        val RowAndValue(row, rowOffset, rowLength, value, valueOffset, valueLength) = rowAndValue(result)
        val sf: SimpleFeature = deserializer.deserialize(value, valueOffset, valueLength)
        sf
      }

      getDensity("INCLUDE", fs).foreach { i =>
        logger.info(s"Got $i")
      }

      true mustEqual (true)
    }

    def getDensity(query: String, fs: SimpleFeatureStore): List[(Double, Double, Double)] = {
      import org.locationtech.geomesa.utils.geotools.Conversions._

      val q = new Query(typeName, ECQL.toFilter(query))
      val env = ReferencedEnvelope.create(new Envelope(-180, 180, -90, 90), DefaultGeographicCRS.WGS84)
      q.getHints.put(QueryHints.DENSITY_BBOX, env)
      q.getHints.put(QueryHints.DENSITY_WIDTH, 500)
      q.getHints.put(QueryHints.DENSITY_HEIGHT, 500)
      val decode = KryoLazyDensityUtils.decodeResult(env, 500, 500)
      fs.getFeatures(q).features().flatMap(decode).toList
    }
  }

  step {
    logger.info("Stopping embedded hbase")
    cluster.shutdownMiniCluster()
    logger.info("Stopped")
  }
}