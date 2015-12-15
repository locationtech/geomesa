/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import com.vividsolutions.jts.geom.Envelope
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.data.{DataStore, DataUtilities, Query}
import org.geotools.factory.Hints
import org.geotools.filter.text.ecql.ECQL
import org.geotools.filter.visitor.ExtractBoundsFilterVisitor
import org.joda.time.{DateTime, DateTimeZone}
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.index.{Constants, QueryHints}
import org.locationtech.geomesa.accumulo.iterators.StatsIterator.{STATS, decodeStat}
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.stats._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class StatsIteratorTest extends Specification {

  sequential

  import org.locationtech.geomesa.utils.geotools.Conversions._

  def createDataStore(sft: SimpleFeatureType, i: Int = 0): DataStore = {
    val testTableName = "tdi_test"

    val dsf = new AccumuloDataStoreFactory

    import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreFactory.params._

    val ds = dsf.createDataStore(Map(
      zookeepersParam.key -> "dummy",
      instanceIdParam.key -> f"dummy$i%d",
      userParam.key       -> "user",
      passwordParam.key   -> "pass",
      tableNameParam.key  -> testTableName,
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

    val fs = ds.getFeatureSource("test").asInstanceOf[SimpleFeatureStore]
    fs.addFeatures(DataUtilities.collection(features))
    fs.getTransaction.commit()
    fs
  }

  def getQuery(query: String, statString: String): Query = {
    val q = new Query("test", ECQL.toFilter(query))
    val geom = q.getFilter.accept(ExtractBoundsFilterVisitor.BOUNDS_VISITOR, null).asInstanceOf[Envelope]
    q.getHints.put(QueryHints.STATS_STRING, statString)
    q.getHints.put(QueryHints.RETURN_ENCODED, java.lang.Boolean.TRUE)
    q
  }

  /**
   * Not testing too much here stat-wise, as most of the stat testing is in geomesa-utils
   */
  "StatsIterator" should {
    val spec = "id:java.lang.Integer,attr:java.lang.Long,dtg:Date,geom:Geometry:srid=4326"
    val sft = SimpleFeatureTypes.createType("test", spec)
    val builder = AvroSimpleFeatureFactory.featureBuilder(sft)
    sft.getUserData.put(Constants.SF_PROPERTY_START_TIME, "dtg")
    val ds = createDataStore(sft, 0)
    val encodedFeatures = (0 until 150).toArray.map{
      i => Array(i, i*2, new DateTime("2012-01-01T19:00:00", DateTimeZone.UTC).toDate, "POINT(-77 38)")
    }
    val fs = loadFeatures(ds, sft, encodedFeatures)

    "handle malformed stat strings" in {
      "query 1" in {
        val q = getQuery("attr BETWEEN 0 AND 300 AND BBOX(geom, -80, 33, -70, 40)", "")
        fs.getFeatures(q).features() must beEmpty
      }
      "query 2" in {
        val q = getQuery("attr BETWEEN 0 AND 300 AND BBOX(geom, -80, 33, -70, 40)", "abcd")
        fs.getFeatures(q).features() must beEmpty
      }
      "query 3" in {
        val q = getQuery("attr BETWEEN 0 AND 300 AND BBOX(geom, -80, 33, -70, 40)", "RangeHistogram()")
        fs.getFeatures(q).features() must beEmpty
      }
      "query 4" in {
        val q = getQuery("attr BETWEEN 0 AND 300 AND BBOX(geom, -80, 33, -70, 40)",
          "RangeHistogram(foo,10,2012-01-01T00:00:00.000Z,2012-02-01T00:00:00.000Z)")
        fs.getFeatures(q).features() must beEmpty
      }
      "query 5" in {
        val q = getQuery("attr BETWEEN 0 AND 300 AND BBOX(geom, -80, 33, -70, 40)", "MinMax(geom)")
        fs.getFeatures(q).features() must beEmpty
      }
      "query 6" in {
        val q = getQuery("attr BETWEEN 0 AND 300 AND BBOX(geom, -80, 33, -70, 40)", "MinMax(abcd)")
        fs.getFeatures(q).features() must beEmpty
      }
    }

    "work with the MinMax stat" in {
      val q = getQuery("attr BETWEEN 0 AND 300 AND BBOX(geom, -80, 33, -70, 40)", "MinMax(attr)")
      val results = fs.getFeatures(q).features().toList
      val sf = results.head

      val minMaxStat = decodeStat(sf.getAttribute(STATS).asInstanceOf[String]).asInstanceOf[MinMax[java.lang.Long]]
      minMaxStat.min mustEqual 0
      minMaxStat.max mustEqual 298
    }

    "work with the IteratorStackCounter stat" in {
      val q = getQuery("attr BETWEEN 0 AND 300 AND BBOX(geom, -80, 33, -70, 40)", "IteratorStackCounter")
      val results = fs.getFeatures(q).features().toList
      val sf = results.head

      val isc = decodeStat(sf.getAttribute(STATS).asInstanceOf[String]).asInstanceOf[IteratorStackCounter]
      isc.count mustEqual 1L
    }

    "work with the EnumeratedHistogram stat" in {
      val q = getQuery("attr BETWEEN 0 AND 300 AND BBOX(geom, -80, 33, -70, 40)", "EnumeratedHistogram(id)")
      val results = fs.getFeatures(q).features().toList
      val sf = results.head

      val eh = decodeStat(sf.getAttribute(STATS).asInstanceOf[String]).asInstanceOf[EnumeratedHistogram[java.lang.Integer]]
      eh.frequencyMap.size mustEqual 150
      eh.frequencyMap(0) mustEqual 1
      eh.frequencyMap(149) mustEqual 1
      eh.frequencyMap(150) mustEqual 0
    }

    "work with the RangeHistogram stat" in {
      val q = getQuery("attr BETWEEN 0 AND 300 AND BBOX(geom, -80, 33, -70, 40)", "RangeHistogram(id,5,10,15)")
      val results = fs.getFeatures(q).features().toList
      val sf = results.head

      val rh = decodeStat(sf.getAttribute(STATS).asInstanceOf[String]).asInstanceOf[RangeHistogram[java.lang.Integer]]
      rh.histogram.size mustEqual 5
      rh.histogram(10) mustEqual 1
      rh.histogram(11) mustEqual 1
      rh.histogram(12) mustEqual 1
      rh.histogram(13) mustEqual 1
      rh.histogram(14) mustEqual 1
    }

    "work with multiple stats at once" in {
      val q = getQuery("attr BETWEEN 0 AND 300 AND BBOX(geom, -80, 33, -70, 40)",
        "MinMax(attr);IteratorStackCounter;EnumeratedHistogram(id);RangeHistogram(id,5,10,15)")
      val results = fs.getFeatures(q).features().toList
      val sf = results.head

      val seqStat = decodeStat(sf.getAttribute(STATS).asInstanceOf[String]).asInstanceOf[SeqStat]
      val stats = seqStat.stats
      stats.size mustEqual 4

      val minMax = stats(0).asInstanceOf[MinMax[java.lang.Long]]
      val isc = stats(1).asInstanceOf[IteratorStackCounter]
      val eh = stats(2).asInstanceOf[EnumeratedHistogram[java.lang.Integer]]
      val rh = stats(3).asInstanceOf[RangeHistogram[java.lang.Integer]]

      minMax.min mustEqual 0
      minMax.max mustEqual 298

      isc.count mustEqual 1L

      eh.frequencyMap.size mustEqual 150
      eh.frequencyMap(0) mustEqual 1
      eh.frequencyMap(149) mustEqual 1
      eh.frequencyMap(150) mustEqual 0

      rh.histogram.size mustEqual 5
      rh.histogram(10) mustEqual 1
      rh.histogram(11) mustEqual 1
      rh.histogram(12) mustEqual 1
      rh.histogram(13) mustEqual 1
      rh.histogram(14) mustEqual 1
    }
  }
}
