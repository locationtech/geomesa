/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/


package org.locationtech.geomesa.accumulo.process.rangeHistogram

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
import org.locationtech.geomesa.accumulo.iterators.RangeHistogramIterator.{HISTOGRAM_SERIES, decodeHistogramMap, jsonToHistogramMap}
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class RangeHistogramProcessTest extends Specification {

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

  "RangeHistogramProcess" should {
    val spec = "id:java.lang.Integer,attr:java.lang.Long,dtg:Date,geom:Geometry:srid=4326"
    val sft = SimpleFeatureTypes.createType(sftName, spec)
    val builder = AvroSimpleFeatureFactory.featureBuilder(sft)
    sft.getUserData.put(Constants.SF_PROPERTY_START_TIME, "dtg")
    val ds = createDataStore(sft, 0)
    val encodedFeatures = (0 until 150).toArray.map {
      i => Array(i.toString, "1.0", new DateTime("2012-01-01T19:00:00", DateTimeZone.UTC).toDate, "POINT(-77 38)")
    }
    val fs = loadFeatures(ds, sft, encodedFeatures)

    val intervalStart = 0L
    val intervalEnd = 300L
    val numBuckets = 30
    val attributeName = "attr"

    "reduce total features returned" in {
      val q = getQuery("(attr BETWEEN 0 AND 300) and BBOX(geom, -80, 33, -70, 40)")

      val geomesaRHP = new RangeHistogramProcess
      val results = geomesaRHP.execute(fs.getFeatures(q), intervalStart, intervalEnd, numBuckets, attributeName)
      val allFeatures = results.features()
      val iter = allFeatures.toList

      (iter must not).beNull
      iter.length mustEqual 1
    }

    "reduce total features returned - json" in {
      val q = getQueryJSON("(attr BETWEEN 0 AND 300) and BBOX(geom, -80, 33, -70, 40)")

      val geomesaRHP = new RangeHistogramProcess
      val results = geomesaRHP.execute(fs.getFeatures(q), intervalStart, intervalEnd, numBuckets, attributeName)
      val allFeatures = results.features()
      val iter = allFeatures.toList

      (iter must not).beNull
      iter.length mustEqual 1
    }

    "retrieve accurate histogram when all data has same attribute value" in {
      val q = getQuery("(attr BETWEEN 0 AND 300) and BBOX(geom, -80, 33, -70, 40)")

      val geomesaRHP = new RangeHistogramProcess
      val results = geomesaRHP.execute(fs.getFeatures(q), intervalStart, intervalEnd, numBuckets, attributeName)
      val iter = results.features().toList
      val sf = iter.head
      iter must not beNull

      val histogramMap = decodeHistogramMap(sf.getAttribute(HISTOGRAM_SERIES).asInstanceOf[String])
      val totalCount = histogramMap.map { case (attributeValue, count) => count}.sum

      totalCount mustEqual 150
      histogramMap.size mustEqual 1
    }

    "retrieve accurate histogram when all data has same attribute value - json" in {
      val q = getQueryJSON("(attr BETWEEN 0 AND 300) and BBOX(geom, -80, 33, -70, 40)")

      val geomesaRHP = new RangeHistogramProcess
      val results = geomesaRHP.execute(fs.getFeatures(q), intervalStart, intervalEnd, numBuckets, attributeName)
      val iter = results.features().toList
      val sf = iter.head
      iter must not beNull

      val histogramMap = jsonToHistogramMap(sf.getAttribute(HISTOGRAM_SERIES).asInstanceOf[String])
      val totalCount = histogramMap.map { case (attributeValue, count) => count}.sum

      totalCount mustEqual 150
      histogramMap.size mustEqual 1
    }

    "maintain total irrespective of point" in {
      val ds = createDataStore(sft, 1)
      val encodedFeatures = (0 until 150).toArray.map {
        i => Array(i.toString, "1.0", new DateTime("2012-01-01T19:00:00", DateTimeZone.UTC).toDate, s"POINT(-77.$i 38.$i)")
      }
      val fs = loadFeatures(ds, sft, encodedFeatures)

      val q = getQuery("(attr BETWEEN 0 AND 300) and BBOX(geom, -80, 33, -70, 40)")

      val geomesaRHP = new RangeHistogramProcess
      val results = geomesaRHP.execute(fs.getFeatures(q), intervalStart, intervalEnd, numBuckets, attributeName)
      val sfList = results.features().toList

      val sf = sfList.head
      val histogramMap = decodeHistogramMap(sf.getAttribute(HISTOGRAM_SERIES).asInstanceOf[String])

      val totalCount = histogramMap.map { case (attributeValue, count) => count}.sum

      totalCount mustEqual 150
      histogramMap.size mustEqual 1
    }

    "maintain total irrespective of point - json" in {
      val ds = createDataStore(sft, 2)
      val encodedFeatures = (0 until 150).toArray.map {
        i => Array(i.toString, "1.0", new DateTime("2012-01-01T19:00:00", DateTimeZone.UTC).toDate, s"POINT(-77.$i 38.$i)")
      }
      val fs = loadFeatures(ds, sft, encodedFeatures)

      val q = getQueryJSON("(attr BETWEEN 0 AND 300) and BBOX(geom, -80, 33, -70, 40)")

      val geomesaRHP = new RangeHistogramProcess
      val results = geomesaRHP.execute(fs.getFeatures(q), intervalStart, intervalEnd, numBuckets, attributeName)
      val sfList = results.features().toList

      val sf = sfList.head
      val histogramMap = jsonToHistogramMap(sf.getAttribute(HISTOGRAM_SERIES).asInstanceOf[String])

      val totalCount = histogramMap.map { case (attributeValue, count) => count}.sum

      totalCount mustEqual 150
      histogramMap.size mustEqual 1
    }

    "correctly bin off of an attribute's interval" in {
      val ds = createDataStore(sft, 3)
      val encodedFeatures = (0 until 150).toArray.map {
        i => Array(i.toString, i * 2, new DateTime(s"2012-01-01T19:00:00", DateTimeZone.UTC).toDate, "POINT(-77 38)")
      }
      val fs = loadFeatures(ds, sft, encodedFeatures)

      val q = getQuery("(attr BETWEEN 0 AND 300) and BBOX(geom, -80, 33, -70, 40)")

      val geomesaRHP = new RangeHistogramProcess
      val results = geomesaRHP.execute(fs.getFeatures(q), intervalStart, intervalEnd, numBuckets, attributeName)
      val sf = results.features().toList.head
      val histogramMap = decodeHistogramMap(sf.getAttribute(HISTOGRAM_SERIES).asInstanceOf[String])

      val totalCount = histogramMap.map {
        case (attributeValue, count) =>
          count mustEqual 5L
          count
      }.sum

      totalCount mustEqual 150
      histogramMap.size mustEqual 30
    }

    "correctly bin off of an attributes intervals - json" in {
      val ds = createDataStore(sft, 4)
      val encodedFeatures = (0 until 150).toArray.map {
        i => Array(i.toString, i * 2, new DateTime(s"2012-01-01T19:00:00", DateTimeZone.UTC).toDate, "POINT(-77 38)")
      }
      val fs = loadFeatures(ds, sft, encodedFeatures)

      val q = getQueryJSON("(dtg between '2012-01-01T00:00:00.000Z' AND '2012-01-02T00:00:00.000Z') and BBOX(geom, -80, 33, -70, 40)")

      val geomesaRHP = new RangeHistogramProcess
      val results = geomesaRHP.execute(fs.getFeatures(q), intervalStart, intervalEnd, numBuckets, attributeName)
      val sf = results.features().toList.head
      val histogramMap = jsonToHistogramMap(sf.getAttribute(HISTOGRAM_SERIES).asInstanceOf[String])

      val totalCount = histogramMap.map {
        case (attributeValue, count) =>
          count mustEqual 5L
          count
      }.sum

      totalCount mustEqual 150
      histogramMap.size mustEqual 30
    }

    "query attribute bounds not in DataStore" in {
      val ds = createDataStore(sft, 5)
      val encodedFeatures = (0 until 50).toArray.map {
        i => Array(i.toString, i * 2, new DateTime(s"2012-01-01T00:00:00.000Z", DateTimeZone.UTC).toDate, "POINT(-77 38)")
      }
      val fs = loadFeatures(ds, sft, encodedFeatures)

      val q = getQuery("(attr BETWEEN -20 AND -10) and BBOX(geom, -80, 33, -70, 40)")

      val geomesaRHP = new RangeHistogramProcess
      val results = geomesaRHP.execute(fs.getFeatures(q), intervalStart, intervalEnd, numBuckets, attributeName)
      val sfList = results.features().toList
      sfList.length mustEqual 0
    }

    "query attribute bounds partially in DataStore" in {
      val ds = createDataStore(sft, 5)
      val encodedFeatures = (0 until 150).toArray.map {
        i => Array(i.toString, i * 2, new DateTime(s"2012-01-01T00:00:00.000Z", DateTimeZone.UTC).toDate, "POINT(-77 38)")
      }
      val fs = loadFeatures(ds, sft, encodedFeatures)

      val q = getQuery("(attr BETWEEN 0 AND 150) and BBOX(geom, -80, 33, -70, 40)")

      val geomesaRHP = new RangeHistogramProcess
      val results = geomesaRHP.execute(fs.getFeatures(q), intervalStart, intervalEnd, numBuckets, attributeName)
      val sf = results.features().toList.head
      val histogramMap = decodeHistogramMap(sf.getAttribute(HISTOGRAM_SERIES).asInstanceOf[String])

      val totalCount = histogramMap.map {
        case (attributeValue, count) =>
          if (attributeValue == 150) {
            count mustEqual 1L
          } else {
            count mustEqual 5L
          }
          count
      }.sum

      totalCount mustEqual 76
      histogramMap.size mustEqual 16
    }

    "nothing to query over" in {
      val ds = createDataStore(sft, 6)
      val encodedFeatures = new Array[Array[_]](0)
      val fs = loadFeatures(ds, sft, encodedFeatures)

      val q = getQuery("(attr BETWEEN 0 AND 300) and BBOX(geom, -80, 33, -70, 40)")

      val geomesaRHP = new RangeHistogramProcess
      val results = geomesaRHP.execute(fs.getFeatures(q), intervalStart, intervalEnd, numBuckets, attributeName)
      val sfList = results.features().toList
      sfList.length mustEqual 0
    }

    "nothing to query over - json" in {
      val ds = createDataStore(sft, 7)
      val encodedFeatures = new Array[Array[_]](0)
      val fs = loadFeatures(ds, sft, encodedFeatures)

      val q = getQueryJSON("(attr BETWEEN 0 AND 300) and BBOX(geom, -80, 33, -70, 40)")

      val geomesaRHP = new RangeHistogramProcess
      val results = geomesaRHP.execute(fs.getFeatures(q), intervalStart, intervalEnd, numBuckets, attributeName)
      val sfList = results.features().toList
      sfList.length mustEqual 0
    }
  }
}
