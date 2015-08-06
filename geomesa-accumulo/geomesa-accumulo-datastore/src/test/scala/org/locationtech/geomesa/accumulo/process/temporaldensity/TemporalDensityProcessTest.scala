/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/


package org.locationtech.geomesa.accumulo.process.temporaldensity

import java.util.concurrent.atomic.AtomicBoolean

import com.vividsolutions.jts.geom.Envelope
import org.apache.accumulo.core.client.TableExistsException
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.hadoop.io.Text
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.data.{DataStore, DataUtilities, Query}
import org.geotools.factory.Hints
import org.geotools.filter.text.ecql.ECQL
import org.geotools.filter.visitor.ExtractBoundsFilterVisitor
import org.joda.time.{DateTime, DateTimeZone}
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.index.{Constants, QueryHints}
import org.locationtech.geomesa.accumulo.iterators.TemporalDensityIterator.{TIME_SERIES, decodeTimeSeries, jsonToTimeSeries}
import org.locationtech.geomesa.accumulo.process.temporalDensity.TemporalDensityProcess
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._


@RunWith(classOf[JUnitRunner])
class TemporalDensityProcessTest extends Specification {

  sequential

  import org.locationtech.geomesa.utils.geotools.Conversions._

  private val tableName = "tableTestTDIP"
  private val sftName = "sftTestTDIP"

  def createDataStore(sft: SimpleFeatureType, i: Int = 0): DataStore = {
    val mockInstance = new MockInstance("dummy" + i)
    val c = mockInstance.getConnector("user", new PasswordToken("pass".getBytes))
    try { c.tableOperations.create(tableName) } catch { case e: TableExistsException => }
    val splits = (0 to 99).map {
      s => "%02d".format(s)
    }.map(new Text(_))
    c.tableOperations().addSplits(tableName, new java.util.TreeSet[Text](splits))

    val dsf = new AccumuloDataStoreFactory

    import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreFactory.params._

    val ds = dsf.createDataStore(Map(
      zookeepersParam.key -> "dummy",
      instanceIdParam.key -> f"dummy$i%d",
      userParam.key       -> "user",
      passwordParam.key   -> "pass",
      tableNameParam.key  -> tableName,
      mockParam.key       -> "true"))
    ds.createSchema(sft)
    ds
  }

  def loadFeatures(ds: DataStore, sft: SimpleFeatureType, encodedFeatures: Array[_ <: Array[_]]): SimpleFeatureStore = {
    val builder = AvroSimpleFeatureFactory.featureBuilder(sft)

    def decodeFeature(e: Array[_]): SimpleFeature = {
      val f = builder.buildFeature(e(0).toString, e.asInstanceOf[Array[AnyRef]])
      f.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
      f.getUserData.put(Hints.PROVIDED_FID, e(0).toString)
      f
    }

    val features = encodedFeatures.map(decodeFeature)

    val fs = ds.getFeatureSource(sftName).asInstanceOf[SimpleFeatureStore]
    fs.addFeatures(DataUtilities.collection(features))
    fs.getTransaction.commit()
    fs
  }


  def getQuery(query: String): Query = {
    val q = new Query(sftName, ECQL.toFilter(query))
    val geom = q.getFilter.accept(ExtractBoundsFilterVisitor.BOUNDS_VISITOR, null).asInstanceOf[Envelope]
    q.getHints.put(QueryHints.RETURN_ENCODED, java.lang.Boolean.TRUE)
    q
  }

  def getQueryJSON(query: String): Query = {
    val q = new Query(sftName, ECQL.toFilter(query))
    val geom = q.getFilter.accept(ExtractBoundsFilterVisitor.BOUNDS_VISITOR, null).asInstanceOf[Envelope]
    q
  }

  "TemporalDensityIteratorProcess" should {
    val spec = "id:java.lang.Integer,attr:java.lang.Double,dtg:Date,geom:Geometry:srid=4326"
    val sft = SimpleFeatureTypes.createType(sftName, spec)
    val builder = AvroSimpleFeatureFactory.featureBuilder(sft)
    sft.getUserData.put(Constants.SF_PROPERTY_START_TIME, "dtg")
    val ds = createDataStore(sft,0)
    val encodedFeatures = (0 until 150).toArray.map{
      i => Array(i.toString, "1.0", new DateTime("2012-01-01T19:00:00", DateTimeZone.UTC).toDate, "POINT(-77 38)")
    }

    val startTime = new DateTime("2012-01-01T0:00:00", DateTimeZone.UTC).toDate
    val endTime = new DateTime("2012-01-02T0:00:00", DateTimeZone.UTC).toDate
    val TDNumBuckets = 24
    val fs = loadFeatures(ds, sft, encodedFeatures)
    //  the iterator compresses the results into bins.
    //  there are less than 150 bin because they are all in the same point in time

    "reduce total features returned" in {
      val q = getQuery("(dtg between '2012-01-01T00:00:00.000Z' AND '2012-01-02T00:00:00.000Z') and BBOX(geom, -80, 33, -70, 40)") //time interval spans the whole datastore queried values
      //      val results = fs.getFeatures(q)
      val geomesaTDP = new TemporalDensityProcess
      val results = geomesaTDP.execute(fs.getFeatures(q), startTime, endTime, TDNumBuckets)
      val allFeatures = results.features()
      val iter = allFeatures.toList
      (iter must not).beNull

      iter.length should be equalTo 1 // one simpleFeature returned
    }

    "reduce total features returned - json" in {
      val q = getQueryJSON("(dtg between '2012-01-01T00:00:00.000Z' AND '2012-01-02T00:00:00.000Z') and BBOX(geom, -80, 33, -70, 40)") //time interval spans the whole datastore queried values
      //      val results = fs.getFeatures(q)
      val geomesaTDP = new TemporalDensityProcess
      val results = geomesaTDP.execute(fs.getFeatures(q), startTime, endTime, TDNumBuckets)
      val allFeatures = results.features()
      val iter = allFeatures.toList
      (iter must not).beNull

      iter.length should be equalTo 1 // one simpleFeature returned
    }

    //  checks that all the buckets' weights returned add up to 150
    "maintain total weights of time" in {
      val q = getQuery("(dtg between '2012-01-01T00:00:00.000Z' AND '2012-01-02T00:00:00.000Z') and BBOX(geom, -80, 33, -70, 40)")//time interval spans the whole datastore queried values
      //      val results = fs.getFeatures(q)
      val geomesaTDP = new TemporalDensityProcess
      val results = geomesaTDP.execute(fs.getFeatures(q), startTime, endTime, TDNumBuckets)
      val iter = results.features().toList
      val sf = iter.head.asInstanceOf[SimpleFeature]
      iter must not beNull

      val timeSeries = decodeTimeSeries(sf.getAttribute(TIME_SERIES).asInstanceOf[String])
      val totalCount = timeSeries.map { case (dateTime, count) => count}.sum

      totalCount should be equalTo 150
      timeSeries.size should be equalTo 1
    }

    "maintain total weights of time - json" in {
      val q = getQueryJSON("(dtg between '2012-01-01T00:00:00.000Z' AND '2012-01-02T00:00:00.000Z') and BBOX(geom, -80, 33, -70, 40)")//time interval spans the whole datastore queried values
      //      val results = fs.getFeatures(q)
      val geomesaTDP = new TemporalDensityProcess
      val results = geomesaTDP.execute(fs.getFeatures(q), startTime, endTime, TDNumBuckets)
      val iter = results.features().toList
      val sf = iter.head.asInstanceOf[SimpleFeature]
      iter must not beNull

      val timeSeries = jsonToTimeSeries(sf.getAttribute(TIME_SERIES).asInstanceOf[String])
      val totalCount = timeSeries.map { case (dateTime, count) => count}.sum

      totalCount should be equalTo 150
      timeSeries.size should be equalTo 1
    }

    // The points varied but that should have no effect on the bucketing of the timeseries
    //  Checks that all the buckets weight sum to 150, the original number of rows
    "maintain weight irrespective of point" in {
      val ds = createDataStore(sft, 1)
      val encodedFeatures = (0 until 150).toArray.map {
        i => Array(i.toString, "1.0", new DateTime("2012-01-01T19:00:00", DateTimeZone.UTC).toDate, s"POINT(-77.$i 38.$i)")
      }
      val fs = loadFeatures(ds, sft, encodedFeatures)

      val q = getQuery("(dtg between '2012-01-01T00:00:00.000Z' AND '2012-01-02T00:00:00.000Z') and BBOX(geom, -80, 33, -70, 40)")//time interval spans the whole datastore queried values

      //      val results = fs.getFeatures(q)
      val geomesaTDP = new TemporalDensityProcess
      val results = geomesaTDP.execute(fs.getFeatures(q), startTime, endTime, TDNumBuckets)
      val sfList = results.features().toList

      val sf = sfList.head.asInstanceOf[SimpleFeature]
      val timeSeries = decodeTimeSeries(sf.getAttribute(TIME_SERIES).asInstanceOf[String])

      val total = timeSeries.map { case (dateTime, count) => count}.sum

      total should be equalTo 150
      timeSeries.size should be equalTo 1
    }

    "maintain weight irrespective of point - json" in {
      val ds = createDataStore(sft, 2)
      val encodedFeatures = (0 until 150).toArray.map {
        i => Array(i.toString, "1.0", new DateTime("2012-01-01T19:00:00", DateTimeZone.UTC).toDate, s"POINT(-77.$i 38.$i)")
      }
      val fs = loadFeatures(ds, sft, encodedFeatures)

      val q = getQueryJSON("(dtg between '2012-01-01T00:00:00.000Z' AND '2012-01-02T00:00:00.000Z') and BBOX(geom, -80, 33, -70, 40)")//time interval spans the whole datastore queried values

      //      val results = fs.getFeatures(q)
      val geomesaTDP = new TemporalDensityProcess
      val results = geomesaTDP.execute(fs.getFeatures(q), startTime, endTime, TDNumBuckets)
      val sfList = results.features().toList

      val sf = sfList.head.asInstanceOf[SimpleFeature]
      val timeSeries = jsonToTimeSeries(sf.getAttribute(TIME_SERIES).asInstanceOf[String])

      val total = timeSeries.map { case (dateTime, count) => count}.sum

      total should be equalTo 150
      timeSeries.size should be equalTo 1
    }

    //this test actually varies the time therefore they should be split into seperate buckets
    //checks that there are 24 buckets, and that all the counts across the buckets sum to 150
    "correctly bin off of time intervals" in {
      val ds = createDataStore(sft, 3)
      val encodedFeatures = (0 until 48).toArray.map {
        i => Array(i.toString, "1.0", new DateTime(s"2012-01-01T${i%24}:00:00", DateTimeZone.UTC).toDate, "POINT(-77 38)")
      }
      val fs = loadFeatures(ds, sft, encodedFeatures)

      val q = getQuery("(dtg between '2012-01-01T00:00:00.000Z' AND '2012-01-02T00:00:00.000Z') and BBOX(geom, -80, 33, -70, 40)")///ake the time interval 1 day and the number of buckets 24

      //      val results = fs.getFeatures(q) // returns one simple feature
      val geomesaTDP = new TemporalDensityProcess
      val results = geomesaTDP.execute(fs.getFeatures(q), startTime, endTime, TDNumBuckets)
      val sf = results.features().toList.head.asInstanceOf[SimpleFeature]
      val timeSeries = decodeTimeSeries(sf.getAttribute(TIME_SERIES).asInstanceOf[String])

      val total = timeSeries.map {
        case (dateTime, count) =>
          count should be equalTo 2L
          count}.sum

      total should be equalTo 48
      timeSeries.size should be equalTo 24
    }

    "correctly bin off of time intervals - json" in {
      val ds = createDataStore(sft, 4)
      val encodedFeatures = (0 until 48).toArray.map {
        i => Array(i.toString, "1.0", new DateTime(s"2012-01-01T${i%24}:00:00", DateTimeZone.UTC).toDate, "POINT(-77 38)")
      }
      val fs = loadFeatures(ds, sft, encodedFeatures)

      val q = getQueryJSON("(dtg between '2012-01-01T00:00:00.000Z' AND '2012-01-02T00:00:00.000Z') and BBOX(geom, -80, 33, -70, 40)")///ake the time interval 1 day and the number of buckets 24
      //      val results = fs.getFeatures(q) // returns one simple feature
      val geomesaTDP = new TemporalDensityProcess
      val results = geomesaTDP.execute(fs.getFeatures(q), startTime, endTime, TDNumBuckets)
      val sf = results.features().toList.head.asInstanceOf[SimpleFeature] //is this the time series??? im pretty sure it is
      val timeSeries = jsonToTimeSeries(sf.getAttribute(TIME_SERIES).asInstanceOf[String])

      val total = timeSeries.map {
        case (dateTime, count) =>
          count should be equalTo 2L
          count}.sum

      total should be equalTo 48
      timeSeries.size should be equalTo 24
    }

    "query dtg bounds not in DataStore" in {
      val ds = createDataStore(sft, 5)
      val encodedFeatures = (0 until 48).toArray.map {
        i => Array(i.toString, "1.0", new DateTime(s"2012-02-01T${i%24}:00:00", DateTimeZone.UTC).toDate, "POINT(-77 38)")
      }
      val fs = loadFeatures(ds, sft, encodedFeatures)

      val q = getQuery("(dtg between '2012-01-01T00:00:00.000Z' AND '2012-01-02T00:00:00.000Z') and BBOX(geom, -80, 33, -70, 40)")///ake the time interval 1 day and the number of buckets 24

      val geomesaTDP = new TemporalDensityProcess
      val results = geomesaTDP.execute(fs.getFeatures(q), startTime, endTime, TDNumBuckets)
      val sfList = results.features().toList
      sfList.length should be equalTo 0
    }

    "query dtg bounds not in DataStore - json" in {
      val ds = createDataStore(sft, 6)
      val encodedFeatures = (0 until 48).toArray.map {
        i => Array(i.toString, "1.0", new DateTime(s"2012-02-01T${i%24}:00:00", DateTimeZone.UTC).toDate, "POINT(-77 38)")
      }
      val fs = loadFeatures(ds, sft, encodedFeatures)

      val q = getQueryJSON("(dtg between '2012-01-01T00:00:00.000Z' AND '2012-01-02T00:00:00.000Z') and BBOX(geom, -80, 33, -70, 40)")///ake the time interval 1 day and the number of buckets 24

      val geomesaTDP = new TemporalDensityProcess
      val results = geomesaTDP.execute(fs.getFeatures(q), startTime, endTime, TDNumBuckets)
      val sfList = results.features().toList
      sfList.length should be equalTo 0
    }

    "nothing to query over" in {
      val ds = createDataStore(sft, 7)
      val encodedFeatures = new Array[Array[_]](0)
      val fs = loadFeatures(ds, sft, encodedFeatures)

      val q = getQuery("(dtg between '2012-01-01T00:00:00.000Z' AND '2012-01-02T00:00:00.000Z') and BBOX(geom, -80, 33, -70, 40)")///ake the time interval 1 day and the number of buckets 24

      val geomesaTDP = new TemporalDensityProcess
      val results = geomesaTDP.execute(fs.getFeatures(q), startTime, endTime, TDNumBuckets)
      val sfList = results.features().toList
      sfList.length should be equalTo 0
    }

    "nothing to query over - json" in {
      val ds = createDataStore(sft, 8)
      val encodedFeatures = new Array[Array[_]](0)
      val fs = loadFeatures(ds, sft, encodedFeatures)

      val q = getQueryJSON("(dtg between '2012-01-01T00:00:00.000Z' AND '2012-01-02T00:00:00.000Z') and BBOX(geom, -80, 33, -70, 40)")///ake the time interval 1 day and the number of buckets 24

      val geomesaTDP = new TemporalDensityProcess
      val results = geomesaTDP.execute(fs.getFeatures(q), startTime, endTime, TDNumBuckets)
      val sfList = results.features().toList
      sfList.length should be equalTo 0
    }
  }
}
