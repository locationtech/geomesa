/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.process.stats

import org.geotools.data.Query
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.filter.text.ecql.ECQL
import org.joda.time.{DateTime, DateTimeZone}
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.accumulo.iterators.KryoLazyStatsIterator._
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.stats._
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class KryoLazyStatsIteratorProcessTest extends Specification with TestWithDataStore {

  sequential

  import org.locationtech.geomesa.utils.geotools.Conversions._

  override val spec = "id:java.lang.Integer,attr:java.lang.Long,dtg:Date,*geom:Geometry:srid=4326"

  addFeatures((0 until 150).toArray.map { i =>
    val attrs = Array(i.asInstanceOf[AnyRef], (i * 2).asInstanceOf[AnyRef],
      new DateTime("2012-01-01T19:00:00", DateTimeZone.UTC).toDate, "POINT(-77 38)")
    val sf = new ScalaSimpleFeature(i.toString, sft)
    sf.setAttributes(attrs)
    sf
  })

  def query = new Query(sftName, ECQL.toFilter("dtg DURING 2012-01-01T18:30:00.000Z/2012-01-01T19:30:00.000Z " +
      "AND bbox(geom,-80,35,-75,40)"))

  "StatsIteratorProcess" should {
    val statsIteratorProcess = new StatsIteratorProcess()

    "work with the MinMax stat" in {
      val results = statsIteratorProcess.execute(fs.getFeatures(query), "MinMax(attr)", encode = true)
      val sf = results.features().next

      val minMaxStat = decodeStat(sf.getAttribute(STATS).asInstanceOf[String]).asInstanceOf[MinMax[java.lang.Long]]
      minMaxStat.min mustEqual 0
      minMaxStat.max mustEqual 298
    }

    "work with the IteratorStackCounter stat" in {
      val results = statsIteratorProcess.execute(fs.getFeatures(query), "IteratorStackCounter", encode = true)
      val sf = results.features().next

      val isc = decodeStat(sf.getAttribute(STATS).asInstanceOf[String]).asInstanceOf[IteratorStackCounter]
      isc.count must beGreaterThanOrEqualTo(1L)
    }

    "work with the EnumeratedHistogram stat" in {
      val results = statsIteratorProcess.execute(fs.getFeatures(query), "EnumeratedHistogram(id)", encode = true)
      val sf = results.features().next

      val eh = decodeStat(sf.getAttribute(STATS).asInstanceOf[String]).asInstanceOf[EnumeratedHistogram[java.lang.Integer]]
      eh.frequencyMap.size mustEqual 150
      eh.frequencyMap(0) mustEqual 1
      eh.frequencyMap(149) mustEqual 1
      eh.frequencyMap(150) mustEqual 0
    }

    "work with the RangeHistogram stat" in {
      val results = statsIteratorProcess.execute(fs.getFeatures(query), "RangeHistogram(id,5,10,15)", encode = true)
      val sf = results.features().next

      val rh = decodeStat(sf.getAttribute(STATS).asInstanceOf[String]).asInstanceOf[RangeHistogram[java.lang.Integer]]
      rh.histogram.size mustEqual 5
      rh.histogram(10) mustEqual 1
      rh.histogram(11) mustEqual 1
      rh.histogram(12) mustEqual 1
      rh.histogram(13) mustEqual 1
      rh.histogram(14) mustEqual 1
    }

    "work with multiple stats at once" in {
      val results = statsIteratorProcess.execute(fs.getFeatures(query),
        "MinMax(attr);IteratorStackCounter;EnumeratedHistogram(id);RangeHistogram(id,5,10,15)", encode = true)
      val sf = results.features().next

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

    "work with non AccumuloFeatureCollections" in {
      val features: DefaultFeatureCollection = new DefaultFeatureCollection(null, sft)
      fs.getFeatures(new Query(sftName, Filter.INCLUDE)).features().foreach(features.add)

      val results = statsIteratorProcess.execute(features,
        "MinMax(attr);IteratorStackCounter;EnumeratedHistogram(id);RangeHistogram(id,5,10,15)", encode = true)
      val sf = results.features().next

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
