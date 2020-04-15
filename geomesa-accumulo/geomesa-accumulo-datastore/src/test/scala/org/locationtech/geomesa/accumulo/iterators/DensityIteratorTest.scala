/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.accumulo.iterators

import java.util.{Date, Properties}

import org.geotools.data.Query
import org.geotools.filter.text.ecql.ECQL
import org.geotools.filter.visitor.ExtractBoundsFilterVisitor
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.geotools.util.Converters
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithFeatureType
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.iterators.DensityScan
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.jts.geom.Envelope
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class DensityIteratorTest extends Specification with TestWithFeatureType {

  import scala.collection.JavaConverters._

  sequential

  override val spec: String =
    "an_id:Int,attr:Double,dtg:Date," +
        "*geom:Point:srid=4326," +
        "testBox:MultiPolygon:srid=4326," +
        "testLine:LineString:srid=4326," +
        "testMultiLine:LineString:srid=4326," +
        "cherryAveSegment:LineString:srid=4326," +
        "cherryAve:MultiLineString:srid=4326," +
        "cville:Polygon:srid=4326"

  lazy val date = Converters.convert("2012-01-01T19:00:00Z", classOf[Date]).getTime

  def getDensity(
      query: String,
      attribute: Option[String] = None,
      envelope: Option[Envelope] = None,
      strategy: Option[GeoMesaFeatureIndex[_, _]] = None): List[(Double, Double, Double)] = {
    val q = new Query(sftName, ECQL.toFilter(query))
    val geom = envelope.getOrElse(q.getFilter.accept(ExtractBoundsFilterVisitor.BOUNDS_VISITOR, null).asInstanceOf[Envelope])
    q.getHints.put(QueryHints.DENSITY_BBOX, new ReferencedEnvelope(geom, DefaultGeographicCRS.WGS84))
    q.getHints.put(QueryHints.DENSITY_WIDTH, 500)
    q.getHints.put(QueryHints.DENSITY_HEIGHT, 500)
    attribute.foreach(q.getHints.put(QueryHints.DENSITY_GEOM, _))
    strategy.foreach(s => q.getHints.put(QueryHints.QUERY_INDEX, s.identifier))
    val decode = DensityScan.decodeResult(geom, 500, 500)
    SelfClosingIterator(ds.getFeatureSource(sftName).getFeatures(q).features).flatMap(decode).toList
  }

  step {
    val testData : Map[String,String] = {
      val dataFile = new Properties
      dataFile.load(getClass.getClassLoader.getResourceAsStream("data/density-iterator.properties"))
      dataFile.asScala.toMap
    }
    val features = Seq.tabulate(15) { i =>
      // space out the points very slightly around 5 primary longitudes 1 degree apart
      val lon = (i / 3) + 1 + (Random.nextDouble() - 0.5) / 1000.0
      val sf = new ScalaSimpleFeature(sft, i.toString)
      sf.setAttribute("an_id", i.toString)
      sf.setAttribute("attr", "1.0")
      sf.setAttribute("dtg", new Date(date + i * 60000))
      sf.setAttribute("geom", s"POINT($lon 37)")
      sf.setAttribute("testBox", testData("[MULTIPOLYGON] test box"))
      sf.setAttribute("testLine", testData("[LINE] test line"))
      sf.setAttribute("testMultiLine", testData("[LINE] Line to MultiLine segment"))
      sf.setAttribute("cherryAveSegment", testData("[LINE] Cherry Avenue segment"))
      sf.setAttribute("cherryAve", testData("[MULTILINE] Cherry Avenue entirety"))
      sf.setAttribute("cville", testData("[POLYGON] Charlottesville"))
      sf
    }

    addFeatures(features)
  }

  "DensityIterator" should {

    "do density calc on points" >> {
      val queries = Seq(
        "BBOX(geom, -1, 33, 6, 40)",
        "BBOX(geom, -1, 33, 6, 40) AND " +
            "dtg between '2012-01-01T18:00:00.000Z' AND '2012-01-01T23:00:00.000Z'"
      )
      foreach(queries) { q =>
        val density = getDensity(q)
        density.length must beLessThan(15)
        density.map(_._3).sum mustEqual 15
        val compiled = density.groupBy(d => (d._1, d._2)).map { case (_, group) => group.map(_._3).sum }
        // should be 5 bins of 30
        compiled must haveLength(5)
        forall(compiled)(_ mustEqual 3)
      }
    }

    "do density calc on a realistic polygon" >> {
      val queries = Seq(
        "BBOX(cville, -78.598118, 37.992204, -78.337364, 38.091238)",
        "BBOX(cville, -78.598118, 37.992204, -78.337364, 38.091238) AND " +
            "dtg between '2012-01-01T18:00:00.000Z' AND '2012-01-01T23:00:00.000Z'"
      )
      foreach(queries) { q =>
        val density = getDensity(q, Some("cville"))
        density.map(_._3).sum must beGreaterThan(0.0)
      }
    }

    "do density calc on a realistic multilinestring" >> {
      val queries = Seq(
        "BBOX(cherryAve, -78.511236, 38.019947, -78.485830, 38.030265)",
        "BBOX(cherryAve, -78.511236, 38.019947, -78.485830, 38.030265) AND " +
            "dtg between '2012-01-01T18:00:00.000Z' AND '2012-01-01T23:00:00.000Z'"
      )
      foreach(queries) { q =>
        val density = getDensity(q, Some("cherryAve"))
        density.map(_._3).sum must beGreaterThan(0.0)
      }
    }

    "do density calc on a realistic linestring" >> {
      val queries = Seq(
        "BBOX(cherryAveSegment, -78.511236, 38.019947, -78.485830, 38.030265)",
        "BBOX(cherryAveSegment, -78.511236, 38.019947, -78.485830, 38.030265) AND " +
            "dtg between '2012-01-01T18:00:00.000Z' AND '2012-01-01T23:00:00.000Z'"
      )
      foreach(queries) { q =>
        val density = getDensity(q, Some("cherryAveSegment"))
        density.map(_._3).sum must beGreaterThan(0.0)
      }
    }

    "do density calc on a linestring with multiLine intersect" >> {
      val queries = Seq(
        "BBOX(testMultiLine, -78.541236, 38.019947, -78.485830, 38.060265)",
        "BBOX(testMultiLine, -78.511236, 38.019947, -78.485830, 38.030265) AND " +
            "dtg between '2012-01-01T18:00:00.000Z' AND '2012-01-01T23:00:00.000Z'"
      )
      foreach(queries) { q =>
        val density = getDensity(q, Some("testMultiLine"))
        density.map(_._3).sum must beGreaterThan(0.0)
      }
    }

    "do density calc on a simplistic multi polygon" >> {
      val queries = Seq(
        "BBOX(testBox, 0.0, 0.0, 10.0, 10.0)",
        "BBOX(testBox, -1.0, -1.0, 11.0, 11.0) AND " +
            "dtg between '2012-01-01T18:00:00.000Z' AND '2012-01-01T23:00:00.000Z'"
      )
      foreach(queries) { q =>
        val density = getDensity(q, Some("testBox"))
        density.map(_._3).sum must beGreaterThan(0.0)
      }
    }

    "do density calc on a simplistic linestring" >> {
      val queries = Seq(
        "BBOX(testLine, 0.0, 0.0, 10.0, 10.0)",
        "BBOX(testLine, -1.0, -1.0, 11.0, 11.0) AND " +
            "dtg between '2012-01-01T18:00:00.000Z' AND '2012-01-01T23:00:00.000Z'"
      )
      foreach(queries) { q =>
        val density = getDensity(q, Some("testLine"))
        density.map(_._3).sum must beGreaterThan(0.0)
      }
    }

    "correctly apply filters smaller than the envelope" in {
      val q = new Query(sftName, ECQL.toFilter("BBOX(geom, 0.5, 33, 1.5, 40)"))
      val envelope = new ReferencedEnvelope(-180, 180, -90, 90, org.locationtech.geomesa.utils.geotools.CRS_EPSG_4326)
      q.getHints.put(QueryHints.DENSITY_BBOX, envelope)
      q.getHints.put(QueryHints.DENSITY_WIDTH, 500)
      q.getHints.put(QueryHints.DENSITY_HEIGHT, 500)
      val decode = DensityScan.decodeResult(envelope, 500, 500)
      val density = SelfClosingIterator(fs.getFeatures(q).features).flatMap(decode).toList
      density.map(_._3).sum mustEqual 3
    }
  }
}
