/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import org.geotools.data.Query
import org.geotools.filter.text.ecql.ECQL
import org.joda.time.{DateTime, DateTimeZone}
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.accumulo.index.QueryHints
import org.locationtech.geomesa.accumulo.iterators.KryoLazyStatsIterator.{STATS, decodeStat}
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.stats._
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class KryoLazyStatsIteratorTest extends Specification with TestWithDataStore {

  sequential

  import org.locationtech.geomesa.utils.geotools.Conversions._

  override val spec = "idt:java.lang.Integer:index=full,attr:java.lang.Long:index=join,dtg:Date,*geom:Geometry:srid=4326"

  addFeatures((0 until 150).toArray.map { i =>
    val attrs = Array(i.asInstanceOf[AnyRef], (i * 2).asInstanceOf[AnyRef],
      new DateTime("2012-01-01T19:00:00", DateTimeZone.UTC).toDate, "POINT(-77 38)")
    val sf = new ScalaSimpleFeature(i.toString, sft)
    sf.setAttributes(attrs)
    sf
  })

  def getQuery(statString: String): Query = {
    val query = new Query(sftName, ECQL.toFilter("dtg DURING 2012-01-01T18:30:00.000Z/2012-01-01T19:30:00.000Z " +
        "AND bbox(geom,-80,35,-75,40)"))
    query.getHints.put(QueryHints.STATS_KEY, statString)
    query.getHints.put(QueryHints.RETURN_ENCODED_KEY, java.lang.Boolean.TRUE)
    query
  }

  /**
   * Not testing too much here stat-wise, as most of the stat testing is in geomesa-utils
   */
  "StatsIterator" should {

    "work with the MinMax stat" in {
      val q = getQuery("MinMax(attr)")
      val results = fs.getFeatures(q).features().toList
      val sf = results.head

      val minMaxStat = decodeStat(sf.getAttribute(STATS).asInstanceOf[String]).asInstanceOf[MinMax[java.lang.Long]]
      minMaxStat.min mustEqual 0
      minMaxStat.max mustEqual 298
    }

    "work with the IteratorStackCounter stat" in {
      val q = getQuery("IteratorStackCounter")
      val results = fs.getFeatures(q).features().toList
      val sf = results.head

      val isc = decodeStat(sf.getAttribute(STATS).asInstanceOf[String]).asInstanceOf[IteratorStackCounter]
      // note: I don't think there is a defined answer here that isn't implementation specific
      isc.count must beGreaterThanOrEqualTo(1L)
    }

    "work with the EnumeratedHistogram stat" in {
      val q = getQuery("EnumeratedHistogram(idt)")
      val results = fs.getFeatures(q).features().toList
      val sf = results.head

      val eh = decodeStat(sf.getAttribute(STATS).asInstanceOf[String]).asInstanceOf[EnumeratedHistogram[java.lang.Integer]]
      eh.frequencyMap.size mustEqual 150
      eh.frequencyMap(0) mustEqual 1
      eh.frequencyMap(149) mustEqual 1
      eh.frequencyMap(150) mustEqual 0
    }

    "work with the RangeHistogram stat" in {
      val q = getQuery("RangeHistogram(idt,5,10,15)")
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
      val q = getQuery("MinMax(attr);IteratorStackCounter;EnumeratedHistogram(idt);RangeHistogram(idt,5,10,15)")
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

      isc.count must beGreaterThanOrEqualTo(1L)

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

    "work with the stidx index" in {
      val q = getQuery("MinMax(attr)")
      q.setFilter(ECQL.toFilter("bbox(geom,-80,35,-75,40)"))
      val results = fs.getFeatures(q).features().toList
      val sf = results.head

      val minMaxStat = decodeStat(sf.getAttribute(STATS).asInstanceOf[String]).asInstanceOf[MinMax[java.lang.Long]]
      minMaxStat.min mustEqual 0
      minMaxStat.max mustEqual 298
    }

    "work with the record index" in {
      val q = getQuery("MinMax(attr)")
      q.setFilter(ECQL.toFilter("IN(0)"))
      val results = fs.getFeatures(q).features().toList
      val sf = results.head

      val minMaxStat = decodeStat(sf.getAttribute(STATS).asInstanceOf[String]).asInstanceOf[MinMax[java.lang.Long]]
      minMaxStat.min mustEqual 0
      minMaxStat.max mustEqual 0
    }

    "work with the attribute partial index" in {
      val q = getQuery("MinMax(attr)")
      q.setFilter(ECQL.toFilter("attr > 10"))
      val results = fs.getFeatures(q).features().toList
      val sf = results.head

      val minMaxStat = decodeStat(sf.getAttribute(STATS).asInstanceOf[String]).asInstanceOf[MinMax[java.lang.Long]]
      minMaxStat.min mustEqual 12
      minMaxStat.max mustEqual 298
    }

    "work with the attribute join index" in {
      val q = getQuery("MinMax(idt)")
      q.setFilter(ECQL.toFilter("attr > 10"))
      val results = fs.getFeatures(q).features().toList
      val sf = results.head

      val minMaxStat = decodeStat(sf.getAttribute(STATS).asInstanceOf[String]).asInstanceOf[MinMax[java.lang.Integer]]
      minMaxStat.min mustEqual 6
      minMaxStat.max mustEqual 149
    }

    "work with the attribute full index" in {
      val q = getQuery("MinMax(attr)")
      q.setFilter(ECQL.toFilter("idt > 10"))
      val results = fs.getFeatures(q).features().toList
      val sf = results.head

      val minMaxStat = decodeStat(sf.getAttribute(STATS).asInstanceOf[String]).asInstanceOf[MinMax[java.lang.Long]]
      minMaxStat.min mustEqual 22
      minMaxStat.max mustEqual 298
    }

  }
}
