/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.hbase.data

import java.util.Date

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
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class HBaseDensityFilterTest extends Specification with LazyLogging {

  sequential

  val cluster: HBaseTestingUtility = new HBaseTestingUtility()
  var connection: Connection = null
  var conf: Configuration = cluster.getConfiguration();
  val TEST_FAMILY = "an_id:java.lang.Integer,attr:java.lang.Double,dtg:Date,geom:Point:srid=4326"
  val TEST_HINT = new Hints()

  val typeName = "testpoints"
  val sftName = "test_sft"

  step {
    logger.info("Starting embedded hbase")
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
      "org.locationtech.geomesa.hbase.coprocessor.KryoLazyDensityCoprocessor");
    cluster.startMiniCluster(1)
    connection = cluster.getConnection
    logger.info("Started")
  }

  lazy val params = Map(ConnectionParam.getName -> connection, BigTableNameParam.getName -> sftName)
  lazy val ds = DataStoreFinder.getDataStore(params).asInstanceOf[HBaseDataStore]

  "HBaseDataStore" should {
    "reduce total features returned" in {
      val (sft, fs) = initializeHBaseSchema()
      clearFeatures()

      val toAdd = (0 until 150).map { i =>
        val sf = new ScalaSimpleFeature(i.toString, sft)
        sf.setAttribute(0, i.toString)
        sf.setAttribute(1, "1.0")
        sf.setAttribute(2, new DateTime("2012-01-01T19:00:00", DateTimeZone.UTC).toDate)
        sf.setAttribute(3, "POINT(-77 38)")
        sf
      }

      val features_list = new ListFeatureCollection(sft, toAdd)
      fs.addFeatures(features_list)

      val q = "(dtg between '2012-01-01T18:00:00.000Z' AND '2012-01-01T23:00:00.000Z') and BBOX(geom, -80, 33, -70, 40)"
      val density = getDensity(typeName, q, fs)
      density.length must beLessThan(150)
    }

    "maintain total weight of points" in {
      val (sft, fs) = initializeHBaseSchema()
      clearFeatures()

      val toAdd = (0 until 150).map { i =>
        val sf = new ScalaSimpleFeature(i.toString, sft)
        sf.setAttribute(0, i.toString)
        sf.setAttribute(1, "1.0")
        sf.setAttribute(2, new DateTime("2012-01-01T19:00:00", DateTimeZone.UTC).toDate)
        sf.setAttribute(3, "POINT(-77 38)")
        sf
      }

      val features_list = new ListFeatureCollection(sft, toAdd)
      fs.addFeatures(features_list)

      val q = "(dtg between '2012-01-01T18:00:00.000Z' AND '2012-01-01T23:00:00.000Z') and BBOX(geom, -80, 33, -70, 40)"
      val density = getDensity(typeName, q, fs)
      density.length must beLessThan(150)
    }

    "maintain weights irrespective of dates" in {
      val (sft, fs) = initializeHBaseSchema()
      clearFeatures()

      val date = new DateTime("2012-01-01T19:00:00", DateTimeZone.UTC).toDate.getTime
      val toAdd = (0 until 150).map { i =>
        val sf = new ScalaSimpleFeature(i.toString, sft)
        sf.setAttribute(0, i.toString)
        sf.setAttribute(1, "1.0")
        sf.setAttribute(2, new Date(date + i * 60000))
        sf.setAttribute(3, "POINT(-77 38)")
        sf
      }

      val features_list = new ListFeatureCollection(sft, toAdd)
      fs.addFeatures(features_list)

      val q = "(dtg between '2012-01-01T18:00:00.000Z' AND '2012-01-01T23:00:00.000Z') and BBOX(geom, -80, 33, -70, 40)"
      val density = getDensity(typeName, q, fs)
      density.length must beLessThan(150)
    }

    "correctly bin points" in {
      val (sft, fs) = initializeHBaseSchema()
      clearFeatures()

      val date = new DateTime("2012-01-01T19:00:00", DateTimeZone.UTC).toDate.getTime
      val toAdd = (0 until 150).map { i =>
        // space out the points very slightly around 5 primary latitudes 1 degree apart
        val lat = (i / 30) + 1 + (Random.nextDouble() - 0.5) / 1000.0
        val sf = new ScalaSimpleFeature(i.toString, sft)
        sf.setAttribute(0, i.toString)
        sf.setAttribute(1, "1.0")
        sf.setAttribute(2, new Date(date + i * 60000))
        sf.setAttribute(3, s"POINT($lat 37)")
        sf
      }

      val features_list = new ListFeatureCollection(sft, toAdd)
      fs.addFeatures(features_list)

      val q = "(dtg between '2012-01-01T18:00:00.000Z' AND '2012-01-01T23:00:00.000Z') and BBOX(geom, -1, 33, 6, 40)"
      val density = getDensity(typeName, q, fs)
      density.map(_._3).sum mustEqual 150

      val compiled = density.groupBy(d => (d._1, d._2)).map { case (pt, group) => group.map(_._3).sum }

      // should be 5 bins of 30
      compiled must haveLength(5)
      forall(compiled)(_ mustEqual 30)
    }

    step {
      logger.info("Stopping embedded hbase")
      cluster.shutdownMiniCluster()
      logger.info("Stopped")
    }
  }

  def clearFeatures(): Unit = {
    val writer = ds.getFeatureWriter(typeName, Filter.INCLUDE, Transaction.AUTO_COMMIT)
    while (writer.hasNext) {
      writer.next()
      writer.remove()
    }
    writer.close()
  }

  def initializeHBaseSchema(): (SimpleFeatureType, SimpleFeatureStore)  = {
    ds.removeSchema(typeName)
    ds.getSchema(typeName) must beNull
    ds.createSchema(SimpleFeatureTypes.createType(typeName, TEST_FAMILY))

    val sft = ds.getSchema(typeName)
    sft must not(beNull)
    val fs = ds.getFeatureSource(typeName).asInstanceOf[SimpleFeatureStore]

    (sft, fs)
  }

  def getDensity(typeName: String, query: String, fs: SimpleFeatureStore): List[(Double, Double, Double)] = {
    val q = new Query(typeName, ECQL.toFilter(query))
    val env = ReferencedEnvelope.create(new Envelope(-180, 180, -90, 90), DefaultGeographicCRS.WGS84)
    q.getHints.put(QueryHints.DENSITY_BBOX, env)
    q.getHints.put(QueryHints.DENSITY_WIDTH, 500)
    q.getHints.put(QueryHints.DENSITY_HEIGHT, 500)
    val decode = KryoLazyDensityUtils.decodeResult(env, 500, 500)
    fs.getFeatures(q).features().flatMap(decode).toList
  }

}