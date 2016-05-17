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

  override val spec = "an_id:java.lang.Integer,attr:java.lang.Long,dtg:Date,*geom:Point:srid=4326"

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

      val minMaxStat = decodeStat(sf.getAttribute(0).asInstanceOf[String], sft).asInstanceOf[MinMax[java.lang.Long]]
      minMaxStat.bounds mustEqual (0, 298)
    }

    "work with the IteratotStackCount stat" in {
      val results = statsIteratorProcess.execute(fs.getFeatures(query), "IteratorStackCount()", encode = true)
      val sf = results.features().next

      val isc = decodeStat(sf.getAttribute(0).asInstanceOf[String], sft).asInstanceOf[IteratorStackCount]
      isc.count must beGreaterThanOrEqualTo(1L)
    }

    "work with the Histogram stat" in {
      val results = statsIteratorProcess.execute(fs.getFeatures(query), "Histogram(an_id)", encode = true)
      val sf = results.features().next

      val eh = decodeStat(sf.getAttribute(0).asInstanceOf[String], sft).asInstanceOf[Histogram[java.lang.Integer]]
      eh.size mustEqual 150
      eh.frequency(0) mustEqual 1
      eh.frequency(149) mustEqual 1
      eh.frequency(150) mustEqual 0
    }

    "work with the RangeHistogram stat" in {
      val results = statsIteratorProcess.execute(fs.getFeatures(query), "RangeHistogram(an_id,5,0,149)", encode = true)
      val sf = results.features().next

      val rh = decodeStat(sf.getAttribute(0).asInstanceOf[String], sft).asInstanceOf[RangeHistogram[java.lang.Integer]]
      rh.length mustEqual 5
      forall(0 until 5)(rh.count(_) mustEqual 30)
    }

    "work with multiple stats at once" in {
      val results = statsIteratorProcess.execute(fs.getFeatures(query),
        "MinMax(attr);IteratorStackCount();Histogram(an_id);RangeHistogram(an_id,5,10,14)", encode = true)
      val sf = results.features().next

      val seqStat = decodeStat(sf.getAttribute(0).asInstanceOf[String], sft).asInstanceOf[SeqStat]
      val stats = seqStat.stats
      stats.size mustEqual 4

      val minMax = stats(0).asInstanceOf[MinMax[java.lang.Long]]
      val isc = stats(1).asInstanceOf[IteratorStackCount]
      val eh = stats(2).asInstanceOf[Histogram[java.lang.Integer]]
      val rh = stats(3).asInstanceOf[RangeHistogram[java.lang.Integer]]

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

    "work with non AccumuloFeatureCollections" in {
      val features: DefaultFeatureCollection = new DefaultFeatureCollection(null, sft)
      fs.getFeatures(new Query(sftName, Filter.INCLUDE)).features().foreach(features.add)

      val results = statsIteratorProcess.execute(features,
        "MinMax(attr);IteratorStackCount();Histogram(an_id);RangeHistogram(an_id,5,10,14)", encode = true)
      val sf = results.features().next

      val seqStat = decodeStat(sf.getAttribute(0).asInstanceOf[String], sft).asInstanceOf[SeqStat]
      val stats = seqStat.stats
      stats.size mustEqual 4

      val minMax = stats(0).asInstanceOf[MinMax[java.lang.Long]]
      val isc = stats(1).asInstanceOf[IteratorStackCount]
      val eh = stats(2).asInstanceOf[Histogram[java.lang.Integer]]
      val rh = stats(3).asInstanceOf[RangeHistogram[java.lang.Integer]]

      minMax.bounds mustEqual (0, 298)

      isc.count mustEqual 1L

      eh.size mustEqual 150
      eh.frequency(0) mustEqual 1
      eh.frequency(149) mustEqual 1
      eh.frequency(150) mustEqual 0

      rh.length mustEqual 5
      rh.bounds mustEqual (0, 149)
      (0 until 5).map(rh.count).sum mustEqual 150
    }
  }
}
