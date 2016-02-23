/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import com.vividsolutions.jts.geom.Envelope
import org.geotools.data.Query
import org.geotools.factory.Hints
import org.geotools.filter.text.ecql.ECQL
import org.geotools.filter.visitor.ExtractBoundsFilterVisitor
import org.joda.time.{DateTime, DateTimeZone, Interval}
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithMultipleSfts
import org.locationtech.geomesa.accumulo.index.QueryHints
import org.locationtech.geomesa.accumulo.iterators.KryoLazyTemporalDensityIterator.{decodeTimeSeries, jsonToTimeSeries}
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureFactory
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner


@RunWith(classOf[JUnitRunner])
class TemporalDensityIteratorTest extends Specification with TestWithMultipleSfts {

  sequential

  def loadFeatures(sft: SimpleFeatureType, encodedFeatures: Array[_ <: Array[_]]): Unit = {
    val builder = AvroSimpleFeatureFactory.featureBuilder(sft)

    def decodeFeature(e: Array[_]): SimpleFeature = {
      val f = builder.buildFeature(e(0).toString, e.asInstanceOf[Array[AnyRef]])
      f.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
      f.getUserData.put(Hints.PROVIDED_FID, e(0).toString)
      f
    }

    addFeatures(sft, encodedFeatures.map(decodeFeature))
  }

  def getFeatures(sft: SimpleFeatureType, query: String, json: Boolean = false): Iterator[SimpleFeature] = {
    import org.locationtech.geomesa.utils.geotools.Conversions._
    val q = new Query(sft.getTypeName, ECQL.toFilter(query))
    val geom = q.getFilter.accept(ExtractBoundsFilterVisitor.BOUNDS_VISITOR, null).asInstanceOf[Envelope]
    q.getHints.put(QueryHints.TEMPORAL_DENSITY_KEY, java.lang.Boolean.TRUE)
    q.getHints.put(QueryHints.TIME_INTERVAL_KEY, new Interval(new DateTime("2012-01-01T0:00:00", DateTimeZone.UTC).getMillis, new DateTime("2012-01-02T0:00:00", DateTimeZone.UTC).getMillis))
    q.getHints.put(QueryHints.TIME_BUCKETS_KEY, 24)
    if (!json) {
      q.getHints.put(QueryHints.RETURN_ENCODED, java.lang.Boolean.TRUE)
    }
    ds.getFeatureSource(sft.getTypeName).getFeatures(q).features()
  }

  val spec = "id:java.lang.Integer,attr:java.lang.Double,dtg:Date,geom:Geometry:srid=4326"
  val sft = createNewSchema(spec)
  val builder = AvroSimpleFeatureFactory.featureBuilder(sft)
  val encodedFeatures = (0 until 144).toArray.map { i =>
    Array(i.toString, "1.0", new DateTime(s"2012-01-01T${i%24}:00:00", DateTimeZone.UTC).toDate, s"POINT(-77.$i 38.$i)")
  }
  loadFeatures(sft, encodedFeatures)

  "TemporalDensityIterator" should {
    "reduce total features returned" >> {
      "with st index" >> {
        val filter = "BBOX(geom, -80, 33, -70, 40)"
        val results = getFeatures(sft, filter)
        results.length must beLessThan(144)
      }

      "with z3 index" >> {
        val filter = "BBOX(geom, -80, 33, -70, 40) AND " +
            "dtg between '2012-01-01T00:00:00.000Z' AND '2012-01-02T00:00:00.000Z'"
        val results = getFeatures(sft, filter)
        results.length must beLessThan(144)
      }
    }

    "maintain total weights of time" >> {
      "with st index" >> {
        val filter = "BBOX(geom, -80, 33, -70, 40)"
        val results = getFeatures(sft, filter)

        val timeSeries = results.flatMap(sf => decodeTimeSeries(sf.getAttribute(0).asInstanceOf[String])).toMap
        val totalCount = timeSeries.values.sum

        totalCount mustEqual 144
      }

      "with z3 index" >> {
        val filter = "BBOX(geom, -80, 33, -70, 40) AND " +
            "dtg between '2012-01-01T00:00:00.000Z' AND '2012-01-02T00:00:00.000Z'"
        val results = getFeatures(sft, filter)

        val timeSeries = results.flatMap(sf => decodeTimeSeries(sf.getAttribute(0).asInstanceOf[String])).toMap
        val totalCount = timeSeries.values.sum

        totalCount mustEqual 144
      }

      "with st index - json" >> {
        val filter = "BBOX(geom, -80, 33, -70, 40)"
        val results = getFeatures(sft, filter, json = true)

        val timeSeries = results.flatMap(sf => jsonToTimeSeries(sf.getAttribute(0).asInstanceOf[String])).toMap
        val totalCount = timeSeries.values.sum

        totalCount mustEqual 144
      }

      "with z3 index - json" >> {
        val filter = "BBOX(geom, -80, 33, -70, 40) AND " +
            "dtg between '2012-01-01T00:00:00.000Z' AND '2012-01-02T00:00:00.000Z'"
        val results = getFeatures(sft, filter, json = true)

        val timeSeries = results.flatMap(sf => jsonToTimeSeries(sf.getAttribute(0).asInstanceOf[String])).toMap
        val totalCount = timeSeries.values.sum

        totalCount mustEqual 144
      }
    }

    "correctly bin off of time intervals" >> {
      "with st index" >> {
        val filter = "BBOX(geom, -80, 33, -70, 40)"
        val results = getFeatures(sft, filter)

        val timeSeries = results.flatMap(sf => decodeTimeSeries(sf.getAttribute(0).asInstanceOf[String])).toMap
        forall(timeSeries.values)(_ mustEqual 6)
        val totalCount = timeSeries.values.sum

        totalCount mustEqual 144
        timeSeries.size mustEqual 24
      }

      "with z3 index" >> {
        val filter = "BBOX(geom, -80, 33, -70, 40) AND " +
            "dtg between '2012-01-01T00:00:00.000Z' AND '2012-01-02T00:00:00.000Z'"
        val results = getFeatures(sft, filter)

        val timeSeries = results.flatMap(sf => decodeTimeSeries(sf.getAttribute(0).asInstanceOf[String])).toMap
        forall(timeSeries.values)(_ mustEqual 6)
        val totalCount = timeSeries.values.sum

        totalCount mustEqual 144
        timeSeries.size mustEqual 24
      }

      "with st index - json" >> {
        val filter = "BBOX(geom, -80, 33, -70, 40)"
        val results = getFeatures(sft, filter, json = true)

        val timeSeries = results.flatMap(sf => jsonToTimeSeries(sf.getAttribute(0).asInstanceOf[String])).toMap
        forall(timeSeries.values)(_ mustEqual 6)
        val totalCount = timeSeries.values.sum

        totalCount mustEqual 144
        timeSeries.size mustEqual 24
      }

      "with z3 index - json" >> {
        val filter = "BBOX(geom, -80, 33, -70, 40) AND " +
            "dtg between '2012-01-01T00:00:00.000Z' AND '2012-01-02T00:00:00.000Z'"
        val results = getFeatures(sft, filter, json = true)

        val timeSeries = results.flatMap(sf => jsonToTimeSeries(sf.getAttribute(0).asInstanceOf[String])).toMap
        forall(timeSeries.values)(_ mustEqual 6)
        val totalCount = timeSeries.values.sum

        totalCount mustEqual 144
        timeSeries.size mustEqual 24
      }
    }

    "encode decode feature" >> {
      val timeSeries = new collection.mutable.HashMap[DateTime, Long]()
      timeSeries.put(new DateTime("2012-01-01T00:00:00", DateTimeZone.UTC), 2)
      timeSeries.put(new DateTime("2012-01-01T01:00:00", DateTimeZone.UTC), 8)

      val encoded = KryoLazyTemporalDensityIterator.encodeTimeSeries(timeSeries)
      val decoded = KryoLazyTemporalDensityIterator.decodeTimeSeries(encoded)

      timeSeries mustEqual decoded
      timeSeries.size mustEqual 2
      timeSeries.get(new DateTime("2012-01-01T00:00:00", DateTimeZone.UTC)).get mustEqual 2L
      timeSeries.get(new DateTime("2012-01-01T01:00:00", DateTimeZone.UTC)).get mustEqual 8L
    }

    "query dtg bounds not >> DataStore" >> {
      "with z3 index" >> {
        val filter = "BBOX(geom, -80, 33, -70, 40) AND " +
            "dtg between '2012-02-01T00:00:00.000Z' AND '2012-02-02T00:00:00.000Z'"
        val results = getFeatures(sft, filter)

        results must beEmpty
      }
    }

    "return empty iterator when nothing to query over" >> {
      val sft = createNewSchema(spec)
      val encodedFeatures = new Array[Array[_]](0)
      loadFeatures(sft, encodedFeatures)

      "with st index" >> {
        val filter = "BBOX(geom, -80, 33, -70, 40)"
        val results = getFeatures(sft, filter)
        results must beEmpty
      }

      "with z3 index" >> {
        val filter = "BBOX(geom, -80, 33, -70, 40) AND " +
            "dtg between '2012-01-01T00:00:00.000Z' AND '2012-01-02T00:00:00.000Z'"
        val results = getFeatures(sft, filter)
        results must beEmpty
      }

      "with st index - json" >> {
        val filter = "BBOX(geom, -80, 33, -70, 40)"
        val results = getFeatures(sft, filter, json = true)
        results must beEmpty
      }

      "with z3 index - json" >> {
        val filter = "BBOX(geom, -80, 33, -70, 40) AND " +
            "dtg between '2012-01-01T00:00:00.000Z' AND '2012-01-02T00:00:00.000Z'"
        val results = getFeatures(sft, filter, json = true)
        results must beEmpty
      }
    }
  }
}
