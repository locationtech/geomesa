/** *********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
* ************************************************************************/

package org.locationtech.geomesa.hbase.data

import java.nio.charset.StandardCharsets

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.Envelope
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call
import org.apache.hadoop.hbase.{HBaseTestingUtility, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback
import org.geotools.data._
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.data.Query
import org.geotools.factory.Hints
import org.geotools.filter.text.ecql.ECQL
import org.geotools.filter.visitor.ExtractBoundsFilterVisitor
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.joda.time.{DateTime, DateTimeZone}
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.hbase.client.FilterAggregatingClient
import org.locationtech.geomesa.hbase.data.HBaseDataStoreFactory.Params._
import org.locationtech.geomesa.hbase.filters.KryoLazyDensityFilter
import org.locationtech.geomesa.hbase.proto.KryoLazyDensityProto.{DensityRequest, DensityResponse, KryoLazyDensityService, Pair => DensityPair}
import org.locationtech.geomesa.hbase.utils.HBaseBatchScan
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.index.ClientSideFiltering.RowAndValue
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class HBaseDensityFilterTest extends Specification with LazyLogging {

  sequential

  val cluster: HBaseTestingUtility = new HBaseTestingUtility()
  var connection: Connection = null
  var conf: Configuration = cluster.getConfiguration();
  val TEST_FAMILY = "an_id:java.lang.Integer,attr:java.lang.Double,dtg:Date,geom:Point:srid=4326"
  val TEST_HINT = new Hints()

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
      val typeName = "testpoints"

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
      //ids.asScala.map(_.getID) must containTheSameElementsAs((0 until 150).map(_.toString))

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

      // 3. Run with coprocessor
      var q = new Query("hello", ECQL.toFilter("(dtg between '2012-01-01T18:00:00.000Z' AND '2012-01-01T23:00:00.000Z') and BBOX(geom, -80, 33, -70, 40)"))
      val geom = q.getFilter.accept(ExtractBoundsFilterVisitor.BOUNDS_VISITOR, null).asInstanceOf[Envelope]
      q.getHints.put(QueryHints.DENSITY_BBOX, new ReferencedEnvelope(geom, DefaultGeographicCRS.WGS84))
      q.getHints.put(QueryHints.DENSITY_WIDTH, 500)
      q.getHints.put(QueryHints.DENSITY_HEIGHT, 500)

      var filter = new KryoLazyDensityFilter(TEST_FAMILY, q.getHints);
      var arr = filter.toByteArray
      val table1 = connection.getTable(table)

      val client = new FilterAggregatingClient()
      val result : List[DensityPair] = client.kryoLazyDensityFilter(table1, arr).asScala.toList

      for (pairs <- result)
        println(pairs.toString)

      true mustEqual (true)
    }
  }

  step {
    logger.info("Stopping embedded hbase")
    cluster.shutdownMiniCluster()
    logger.info("Stopped")
  }
}