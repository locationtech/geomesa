/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.process.analytic

import java.util.Collections

import org.geotools.data.Query
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.iterators.StatsScan
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.stats._
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class StatsProcessTest extends Specification with TestWithDataStore {

  sequential

  override val spec = "an_id:java.lang.Integer,attr:java.lang.Long,dtg:Date,*geom:Point:srid=4326"

  addFeatures((0 until 150).toArray.map { i =>
    val attrs = Array(i.asInstanceOf[AnyRef], (i * 2).asInstanceOf[AnyRef], "2012-01-01T19:00:00Z", "POINT(-77 38)")
    val sf = new ScalaSimpleFeature(sft, i.toString)
    sf.setAttributes(attrs)
    sf
  })

  def query = new Query(sftName, ECQL.toFilter("dtg DURING 2012-01-01T18:30:00.000Z/2012-01-01T19:30:00.000Z " +
      "AND bbox(geom,-80,35,-75,40)"))

  "StatsIteratorProcess" should {
    val statsIteratorProcess = new StatsProcess()

    "work with the MinMax stat" in {
      val results = statsIteratorProcess.execute(fs.getFeatures(query), "MinMax(attr)", encode = true)
      val sf = results.features().next

      val minMaxStat = StatsScan.decodeStat(sft)(sf.getAttribute(0).asInstanceOf[String]).asInstanceOf[MinMax[java.lang.Long]]
      minMaxStat.bounds mustEqual (0, 298)
    }

    "work with the IteratotStackCount stat" in {
      val results = statsIteratorProcess.execute(fs.getFeatures(query), "IteratorStackCount()", encode = true)
      val sf = results.features().next

      val isc = StatsScan.decodeStat(sft)(sf.getAttribute(0).asInstanceOf[String]).asInstanceOf[IteratorStackCount]
      isc.count must beGreaterThanOrEqualTo(1L)
    }

    "work with the Histogram stat" in {
      val results = statsIteratorProcess.execute(fs.getFeatures(query), "Enumeration(an_id)", encode = true)
      val sf = results.features().next

      val eh = StatsScan.decodeStat(sft)(sf.getAttribute(0).asInstanceOf[String]).asInstanceOf[EnumerationStat[java.lang.Integer]]
      eh.size mustEqual 150
      eh.frequency(0) mustEqual 1
      eh.frequency(149) mustEqual 1
      eh.frequency(150) mustEqual 0
    }

    "work with the RangeHistogram stat" in {
      val results = statsIteratorProcess.execute(fs.getFeatures(query), "Histogram(an_id,5,0,149)", encode = true)
      val sf = results.features().next

      val rh = StatsScan.decodeStat(sft)(sf.getAttribute(0).asInstanceOf[String]).asInstanceOf[Histogram[java.lang.Integer]]
      rh.length mustEqual 5
      forall(0 until 5)(rh.count(_) mustEqual 30)
    }

    "work with the GroupBy stat" in {
      val results = statsIteratorProcess.execute(fs.getFeatures(query), "GroupBy(an_id,MinMax(attr))", encode = true)
      val sf = results.features().next
      val gb = StatsScan.decodeStat(sft)(sf.getAttribute(0).asInstanceOf[String]).asInstanceOf[GroupBy[_]]
      gb.size mustEqual 150
    }

    "work with the DescriptiveStats stat" in {
      val results = statsIteratorProcess.execute(fs.getFeatures(query), "DescriptiveStats(attr)", encode = true)
      val sf = results.features().next

      val rh = StatsScan.decodeStat(sft)(sf.getAttribute(0).asInstanceOf[String]).asInstanceOf[DescriptiveStats]
      rh.count mustEqual 150
      rh.bounds(0) mustEqual (0, 298)
      rh.mean(0) must beCloseTo(149.0, 1e-9)
      rh.populationVariance(0) must beCloseTo(7499.666666666667, 1e-9)
      rh.populationStandardDeviation(0) must beCloseTo(86.60061585616275, 1e-9)
      rh.populationSkewness(0) must beCloseTo(0.0, 1e-9)
      rh.populationKurtosis(0) must beCloseTo(1.7998933285923824, 1e-9)
      rh.populationExcessKurtosis(0) must beCloseTo(-1.2001066714076176, 1e-9)
      rh.sampleVariance(0) must beCloseTo(7550.0, 1e-9)
      rh.sampleStandardDeviation(0) must beCloseTo(86.89073598491383, 1e-9)
      rh.sampleSkewness(0) must beCloseTo(0.0, 1e-9)
      rh.sampleKurtosis(0) must beCloseTo(1.859889772878795, 1e-9)
      rh.sampleExcessKurtosis(0) must beCloseTo(-1.140110227121205, 1e-9)
      rh.populationCovariance(0) must beCloseTo(7499.666666666667, 1e-9)
      rh.populationCorrelation(0) must beCloseTo(1.0, 1e-9)
      rh.sampleCovariance(0) must beCloseTo(7550.0, 1e-9)
      rh.sampleCorrelation(0) must beCloseTo(1.0, 1e-9)
    }

    "work with multiple stats at once" in {
      val results = statsIteratorProcess.execute(fs.getFeatures(query),
        "MinMax(attr);IteratorStackCount();Enumeration(an_id);Histogram(an_id,5,10,14)", encode = true)
      val sf = results.features().next

      val seqStat = StatsScan.decodeStat(sft)(sf.getAttribute(0).asInstanceOf[String]).asInstanceOf[SeqStat]
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

    "work with non AccumuloFeatureCollections" in {
      val features: DefaultFeatureCollection = new DefaultFeatureCollection(null, sft)
      SelfClosingIterator(fs.getFeatures(new Query(sftName, Filter.INCLUDE)).features).foreach(features.add)

      val results = statsIteratorProcess.execute(features,
        "MinMax(attr);IteratorStackCount();Enumeration(an_id);Histogram(an_id,5,10,14)", encode = true)
      val sf = results.features().next

      val seqStat = StatsScan.decodeStat(sft)(sf.getAttribute(0).asInstanceOf[String]).asInstanceOf[SeqStat]
      val stats = seqStat.stats
      stats.size mustEqual 4

      val minMax = stats(0).asInstanceOf[MinMax[java.lang.Long]]
      val isc = stats(1).asInstanceOf[IteratorStackCount]
      val eh = stats(2).asInstanceOf[EnumerationStat[java.lang.Integer]]
      val rh = stats(3).asInstanceOf[Histogram[java.lang.Integer]]

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

    "return stats encoded as json" in {
      val results = statsIteratorProcess.execute(fs.getFeatures(query), "MinMax(attr)", false)
      val sf = results.features().next

      val expectedOutput = """{ "min": 0, "max": 298, "cardinality": 152 }"""
      sf.getAttribute(0) must beEqualTo(expectedOutput).ignoreSpace
    }

    "return stats encoded as json with non-Accumulo Feature collections" in {
      val features: DefaultFeatureCollection = new DefaultFeatureCollection(null, sft)
      SelfClosingIterator(fs.getFeatures(new Query(sftName, Filter.INCLUDE)).features).foreach(features.add)

      val results = statsIteratorProcess.execute(features, "MinMax(attr)", false)
      val sf = results.features().next

      val expectedOutput = """{ "min": 0, "max": 298, "cardinality": 152 }"""
      sf.getAttribute(0) must beEqualTo(expectedOutput).ignoreSpace
    }

    "return stats binary encoded as with Accumulo Feature collections" in {
      val results = statsIteratorProcess.execute(fs.getFeatures(query), "MinMax(attr)", true)
      val sf = results.features().next

      val stat = StatsScan.decodeStat(sft)(sf.getAttribute(0).asInstanceOf[String]).asInstanceOf[MinMax[Long]]
      stat.min mustEqual(0)
      stat.max mustEqual(298)
    }


    "return stats binary encoded as with non-Accumulo Feature collections" in {
      val features: DefaultFeatureCollection = new DefaultFeatureCollection(null, sft)
      SelfClosingIterator(fs.getFeatures(new Query(sftName, Filter.INCLUDE)).features).foreach(features.add)

      val results = statsIteratorProcess.execute(features, "MinMax(attr)", true)
      val sf = results.features().next

      val stat = StatsScan.decodeStat(sft)(sf.getAttribute(0).asInstanceOf[String]).asInstanceOf[MinMax[Long]]
      stat.min mustEqual(0)
      stat.max mustEqual(298)
    }

    "return transforms stats encoded as json" in {
      val results = statsIteratorProcess.execute(fs.getFeatures(query), "MinMax(attr1)", false, Collections.singletonList("attr1=attr+5"))
      val sf = results.features().next

      // NB: Doubles <=> Ints:(
      val expectedOutput = """{ "min": 5.0, "max": 303.0, "cardinality": 149 }"""
      sf.getAttribute(0) must beEqualTo(expectedOutput).ignoreSpace
    }

    "return transforms stats encoded as json with non AccumuloFeatureCollections" in {
      val features: DefaultFeatureCollection = new DefaultFeatureCollection(null, sft)
      SelfClosingIterator(fs.getFeatures(new Query(sftName, Filter.INCLUDE)).features).foreach(features.add)

      val results = statsIteratorProcess.execute(features, "MinMax(attr1)", false, Collections.singletonList("attr1=attr+5"))
      val sf = results.features().next

      // NB: Doubles <=> Ints:(
      val expectedOutput = """{ "min": 5.0, "max": 303.0, "cardinality": 149 }"""
      sf.getAttribute(0) must beEqualTo(expectedOutput).ignoreSpace
    }
  }
}
