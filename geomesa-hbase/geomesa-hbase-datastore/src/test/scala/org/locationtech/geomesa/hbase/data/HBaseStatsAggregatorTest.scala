/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.data

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.{DataStoreFinder, Query, Transaction}
import org.geotools.filter.text.ecql.ECQL
import org.geotools.util.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.iterators.StatsScan
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.stats._
import org.opengis.feature.simple.SimpleFeatureType
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class HBaseStatsAggregatorTest extends Specification with LazyLogging {

  import org.locationtech.geomesa.index.iterators.StatsScan.decodeStat

  sequential

  val TEST_FAMILY = "idt:java.lang.Integer:index=full,attr:java.lang.Long:index=true,dtg:Date,*geom:Point:srid=4326"
  val TEST_HINT = new Hints()
  val typeName = "HBaseStatsAggregatorTest"

  lazy val features = (0 until 150).map { i =>
    val sf = ScalaSimpleFeature.create(sft, i.toString, i, i * 2, "2012-01-01T19:00:00Z", "POINT(-77 38)")
    sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
    sf
  }

  lazy val params = Map(
    HBaseDataStoreParams.ConnectionParam.key       -> MiniCluster.connection,
    HBaseDataStoreParams.HBaseCatalogParam.key     -> getClass.getSimpleName,
    HBaseDataStoreParams.StatsCoprocessorParam.key -> true
  )

  lazy val ds = DataStoreFinder.getDataStore(params).asInstanceOf[HBaseDataStore]
  lazy val dsSemiLocal = DataStoreFinder.getDataStore(params ++ Map(HBaseDataStoreParams.StatsCoprocessorParam.key -> false)).asInstanceOf[HBaseDataStore]
  lazy val dsFullLocal = DataStoreFinder.getDataStore(params ++ Map(HBaseDataStoreParams.RemoteFilteringParam.key -> false)).asInstanceOf[HBaseDataStore]
  lazy val dsThreads1 = DataStoreFinder.getDataStore(params ++ Map(HBaseDataStoreParams.CoprocessorThreadsParam.key -> "1")).asInstanceOf[HBaseDataStore]
  lazy val dsYieldPartials = DataStoreFinder.getDataStore(params ++ Map(HBaseDataStoreParams.YieldPartialResultsParam.key -> true)).asInstanceOf[HBaseDataStore]
  lazy val dataStores = Seq(ds, dsSemiLocal, dsFullLocal, dsThreads1, dsYieldPartials)

  var sft: SimpleFeatureType = _

  step {
    logger.info("Starting the Stats Aggregator Test")
    ds.getSchema(typeName) must beNull
    ds.createSchema(SimpleFeatureTypes.createType(typeName, TEST_FAMILY))
    sft = ds.getSchema(typeName)
    WithClose(ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT)) { writer =>
      features.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
    }
  }

  "HBaseDataStoreFactory" should {
    "enable coprocessors" in {
      ds.config.remoteFilter must beTrue
      ds.config.coprocessors.enabled.stats must beTrue
      dsSemiLocal.config.remoteFilter must beTrue
      dsSemiLocal.config.coprocessors.enabled.stats must beFalse
      dsFullLocal.config.remoteFilter must beFalse
    }
  }

  /**
   * Not testing too much here stat-wise, as most of the stat testing is in geomesa-utils
   */
  "StatsIterator" should {
    "work with the MinMax stat" in {
      foreach(dataStores) { ds =>
        val q = getQuery("MinMax(attr)")
        val results = SelfClosingIterator(ds.getFeatureReader(q, Transaction.AUTO_COMMIT)).toList
        val sf = results.head
        val minMaxStat = decodeStat(sft)(sf.getAttribute(0).asInstanceOf[String]).asInstanceOf[MinMax[java.lang.Long]]
        minMaxStat.bounds mustEqual (0, 298)
      }
    }

    "work with the IteratorStackCount stat" in {
      foreach(dataStores) { ds =>
        val q = getQuery("IteratorStackCount()")
        val results = SelfClosingIterator(ds.getFeatureReader(q, Transaction.AUTO_COMMIT)).toList
        val sf = results.head

        val isc = decodeStat(sft)(sf.getAttribute(0).asInstanceOf[String]).asInstanceOf[IteratorStackCount]
        // note: I don't think there is a defined answer here that isn't implementation specific
        isc.count must beGreaterThanOrEqualTo(1L)
      }
    }

    "work with the Enumeration stat" in {
      foreach(dataStores) { ds =>
        val q = getQuery("Enumeration(idt)")
        val results = SelfClosingIterator(ds.getFeatureReader(q, Transaction.AUTO_COMMIT)).toList
        val sf = results.head

        val eh = decodeStat(sft)(sf.getAttribute(0).asInstanceOf[String]).asInstanceOf[EnumerationStat[java.lang.Integer]]
        eh.size mustEqual 150
        eh.frequency(0) mustEqual 1
        eh.frequency(149) mustEqual 1
        eh.frequency(150) mustEqual 0
      }
    }

    "work with the Histogram stat" in {
      foreach(dataStores) { ds =>
        val q = getQuery("Histogram(idt,5,10,14)", Some("idt between 10 and 14"))
        val results = SelfClosingIterator(ds.getFeatureReader(q, Transaction.AUTO_COMMIT)).toList
        val sf = results.head

        val rh = decodeStat(sft)(sf.getAttribute(0).asInstanceOf[String]).asInstanceOf[Histogram[java.lang.Integer]]
        rh.length mustEqual 5
        rh.count(rh.indexOf(10)) mustEqual 1
        rh.count(rh.indexOf(11)) mustEqual 1
        rh.count(rh.indexOf(12)) mustEqual 1
        rh.count(rh.indexOf(13)) mustEqual 1
        rh.count(rh.indexOf(14)) mustEqual 1
      }
    }

    "work with the Histogram and Count stats" in {
      foreach(dataStores) { ds =>
        val q = getQuery("Histogram(idt,5,10,14);Count()", Some("idt between 10 and 14"))
        val results = SelfClosingIterator(ds.getFeatureReader(q, Transaction.AUTO_COMMIT)).toList
        val sf = results.head

        val ss = decodeStat(sft)(sf.getAttribute(0).asInstanceOf[String]).asInstanceOf[SeqStat]

        val rh = ss.stats(0).asInstanceOf[Histogram[java.lang.Integer]]
        rh.length mustEqual 5
        rh.count(rh.indexOf(10)) mustEqual 1
        rh.count(rh.indexOf(11)) mustEqual 1
        rh.count(rh.indexOf(12)) mustEqual 1
        rh.count(rh.indexOf(13)) mustEqual 1
        rh.count(rh.indexOf(14)) mustEqual 1

        val ch = ss.stats(1).asInstanceOf[CountStat]
        ch.count mustEqual 5
      }
    }

    "work with the count stat" in {
      foreach(dataStores) { ds =>
        val q = getQuery("Count()", Some("idt between 10 and 14"))
        val results = SelfClosingIterator(ds.getFeatureReader(q, Transaction.AUTO_COMMIT)).toList
        val sf = results.head

        val ch = decodeStat(sft)(sf.getAttribute(0).asInstanceOf[String]).asInstanceOf[CountStat]

        ch.count mustEqual 5
      }
    }

    "work with multiple batches" in {
      StatsScan.BatchSize.threadLocalValue.set("20")
      try {
        foreach(dataStores) { ds =>
          val q = getQuery("Count()", None)
          val results = SelfClosingIterator(ds.getFeatureReader(q, Transaction.AUTO_COMMIT)).toList
          val sf = results.head
          val ch = decodeStat(sft)(sf.getAttribute(0).asInstanceOf[String]).asInstanceOf[CountStat]
          ch.count mustEqual 150
        }
      } finally {
        StatsScan.BatchSize.threadLocalValue.remove()
      }
    }

    "work with multiple stats at once" in {
      foreach(dataStores) { ds =>
        val q = getQuery("MinMax(attr);IteratorStackCount();Enumeration(idt);Histogram(idt,5,10,14)")
        val results = SelfClosingIterator(ds.getFeatureReader(q, Transaction.AUTO_COMMIT)).toList
        val sf = results.head

        val seqStat = decodeStat(sft)(sf.getAttribute(0).asInstanceOf[String]).asInstanceOf[SeqStat]
        val stats = seqStat.stats
        stats.size mustEqual 4

        val minMax = stats(0).asInstanceOf[MinMax[java.lang.Long]]
        val isc = stats(1).asInstanceOf[IteratorStackCount]
        val eh = stats(2).asInstanceOf[EnumerationStat[java.lang.Integer]]
        val rh = stats(3).asInstanceOf[Histogram[java.lang.Integer]]

        minMax.bounds mustEqual (0, 298)

        isc.count must beGreaterThanOrEqualTo(1L)

        eh.size mustEqual 150
        eh.frequency(0) mustEqual 1
        eh.frequency(149) mustEqual 1
        eh.frequency(150) mustEqual 0

        rh.length mustEqual 5
        rh.bounds mustEqual (0, 149)
        (0 until 5).map(rh.count).sum mustEqual 150
      }
    }

    "work with the z2 index" in {
      foreach(dataStores) { ds =>
        val q = getQuery("MinMax(attr)")
        q.setFilter(ECQL.toFilter("bbox(geom,-80,35,-75,40)"))
        val results = SelfClosingIterator(ds.getFeatureReader(q, Transaction.AUTO_COMMIT)).toList
        val sf = results.head

        val minMaxStat = decodeStat(sft)(sf.getAttribute(0).asInstanceOf[String]).asInstanceOf[MinMax[java.lang.Long]]
        minMaxStat.bounds mustEqual (0, 298)
      }
    }

    "work with the id index" in {
      foreach(dataStores) { ds =>
        val q = getQuery("MinMax(attr)")
        q.setFilter(ECQL.toFilter("IN('149', '100')"))
        val results = SelfClosingIterator(ds.getFeatureReader(q, Transaction.AUTO_COMMIT)).toList
        val sf = results.head

        val minMaxStat = decodeStat(sft)(sf.getAttribute(0).asInstanceOf[String]).asInstanceOf[MinMax[java.lang.Long]]
        minMaxStat.bounds mustEqual (200, 298)
      }
    }

    "work with the attribute index" in {
      foreach(dataStores) { ds =>
        val q = getQuery("MinMax(attr)")
        q.setFilter(ECQL.toFilter("attr > 10"))
        val results = SelfClosingIterator(ds.getFeatureReader(q, Transaction.AUTO_COMMIT)).toList
        val sf = results.head

        val minMaxStat = decodeStat(sft)(sf.getAttribute(0).asInstanceOf[String]).asInstanceOf[MinMax[java.lang.Long]]
        minMaxStat.bounds mustEqual (12, 298)
      }
    }

    "work with the attribute index on other fields" in {
      foreach(dataStores) { ds =>
        val q = getQuery("MinMax(idt)")
        q.setFilter(ECQL.toFilter("attr > 10"))
        val results = SelfClosingIterator(ds.getFeatureReader(q, Transaction.AUTO_COMMIT)).toList
        val sf = results.head

        val minMaxStat = decodeStat(sft)(sf.getAttribute(0).asInstanceOf[String]).asInstanceOf[MinMax[java.lang.Integer]]
        minMaxStat.bounds mustEqual (6, 149)
      }
    }

    "work with the attribute index on flipped fields" in {
      foreach(dataStores) { ds =>
        val q = getQuery("MinMax(attr)")
        q.setFilter(ECQL.toFilter("idt > 10"))
        val results = SelfClosingIterator(ds.getFeatureReader(q, Transaction.AUTO_COMMIT)).toList
        val sf = results.head

        val minMaxStat = decodeStat(sft)(sf.getAttribute(0).asInstanceOf[String]).asInstanceOf[MinMax[java.lang.Long]]
        minMaxStat.bounds mustEqual (22, 298)
      }
    }

    "handle empty queries with exact stats" >> {
      foreach(dataStores) { ds =>
        val filter = "dtg > '2019-01-01T00:00:00.000Z' AND dtg < '2019-01-02T00:00:00.000Z' AND dtg > currentDate('-P1D')"
        val calculated = ds.stats.getCount(sft, ECQL.toFilter(filter), exact = true)
        calculated must beSome(0L)
      }
    }
  }

  step {
    dataStores.foreach { _.dispose() }
  }

  def getQuery(statString: String, ecql: Option[String] = None): Query = {
    val query = new Query(typeName, ECQL.toFilter("dtg DURING 2012-01-01T18:30:00.000Z/2012-01-01T19:30:00.000Z " +
      "AND bbox(geom,-80,35,-75,40)" + ecql.map(" AND " + _).getOrElse("")))
    query.getHints.put(QueryHints.STATS_STRING, statString)
    query.getHints.put(QueryHints.ENCODE_STATS, java.lang.Boolean.TRUE)
    query
  }
}
