/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.accumulo.iterators

import java.util.{Date, Properties}

import org.locationtech.jts.geom.Envelope
import org.geotools.data.Query
import org.geotools.filter.text.ecql.ECQL
import org.geotools.filter.visitor.ExtractBoundsFilterVisitor
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.geotools.util.Converters
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithMultipleSfts
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.index.id.IdIndex
import org.locationtech.geomesa.index.iterators.DensityScan
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.opengis.feature.simple.SimpleFeatureType
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class DensityIteratorTest extends Specification with TestWithMultipleSfts {

  sequential

  val testData : Map[String,String] = {
    val dataFile = new Properties
    dataFile.load(getClass.getClassLoader.getResourceAsStream("data/density-iterator.properties"))
    dataFile.toMap
  }
  val date = Converters.convert("2012-01-01T19:00:00Z", classOf[Date]).getTime

  def spec(binding: String) = s"an_id:java.lang.Integer,attr:java.lang.Double,dtg:Date,*geom:$binding:srid=4326"

  def getDensity(sftName: String,
                 query: String,
                 envelope: Option[Envelope] = None,
                 strategy: Option[GeoMesaFeatureIndex[_, _]] = None): List[(Double, Double, Double)] = {
    val q = new Query(sftName, ECQL.toFilter(query))
    val geom = envelope.getOrElse(q.getFilter.accept(ExtractBoundsFilterVisitor.BOUNDS_VISITOR, null).asInstanceOf[Envelope])
    q.getHints.put(QueryHints.DENSITY_BBOX, new ReferencedEnvelope(geom, DefaultGeographicCRS.WGS84))
    q.getHints.put(QueryHints.DENSITY_WIDTH, 500)
    q.getHints.put(QueryHints.DENSITY_HEIGHT, 500)
    strategy.foreach(s => q.getHints.put(QueryHints.QUERY_INDEX, s.identifier))
    val decode = DensityScan.decodeResult(geom, 500, 500)
    SelfClosingIterator(ds.getFeatureSource(sftName).getFeatures(q).features).flatMap(decode).toList
  }

  "DensityIterator" should {
    "do density calc on points" >> {

      var sft: SimpleFeatureType = null

      "add features" >> {
        sft = createNewSchema(spec("Point"))
        val features =
        addFeatures(sft, (0 until 150).map { i =>
          // space out the points very slightly around 5 primary longitudes 1 degree apart
          val lon = (i / 30) + 1 + (Random.nextDouble() - 0.5) / 1000.0
          val sf = new ScalaSimpleFeature(sft, i.toString)
          sf.setAttribute(0, i.toString)
          sf.setAttribute(1, "1.0")
          sf.setAttribute(2, new Date(date + i * 60000))
          sf.setAttribute(3, s"POINT($lon 37)")
          sf
        })
        ok
      }

      "with spatial index" >> {
        val q = "BBOX(geom, -1, 33, 6, 40)"
        val density = getDensity(sft.getTypeName, q)

        density.length must beLessThan(150)
        density.map(_._3).sum mustEqual 150
        val compiled = density.groupBy(d => (d._1, d._2)).map { case (pt, group) => group.map(_._3).sum }
        // should be 5 bins of 30
        compiled must haveLength(5)
        forall(compiled)(_ mustEqual 30)
      }

      "with spatio-temporal index" >> {
        val q = "BBOX(geom, -1, 33, 6, 40) AND " +
            "dtg between '2012-01-01T18:00:00.000Z' AND '2012-01-01T23:00:00.000Z'"
        val density = getDensity(sft.getTypeName, q)

        density.length must beLessThan(150)
        density.map(_._3).sum mustEqual 150
        val compiled = density.groupBy(d => (d._1, d._2)).map { case (pt, group) => group.map(_._3).sum }
        // should be 5 bins of 30
        compiled must haveLength(5)
        forall(compiled)(_ mustEqual 30)
      }

      "with record index" >> {
        val q = "INCLUDE"
        val idIndex = ds.manager.indices(sft).find(_.name == IdIndex.name)
        idIndex must beSome
        val density = getDensity(sft.getTypeName, q, Some(new Envelope(-180, 180, -90, 90)), idIndex)

        density.length must beLessThan(150)
        density.map(_._3).sum mustEqual 150
        val compiled = density.groupBy(d => (d._1, d._2)).map { case (pt, group) => group.map(_._3).sum }
        // should be 5 bins of 30
        compiled must haveLength(5)
        forall(compiled)(_ mustEqual 30)
      }
    }

    "do density calc on a realistic polygon" >> {

      var sft: SimpleFeatureType = null

      "add features" >> {
        sft = createNewSchema(spec("Polygon"))
        addFeatures(sft, (0 until 15).toArray.map { i =>
          val sf = new ScalaSimpleFeature(sft, i.toString)
          sf.setAttribute(0, i.toString)
          sf.setAttribute(1, "1.0")
          sf.setAttribute(2, new Date(date + i * 60000))
          sf.setAttribute(3, testData("[POLYGON] Charlottesville"))
          sf
        })
        ok
      }

      "with spatial index" >> {
        val q = "BBOX(geom, -78.598118, 37.992204, -78.337364, 38.091238)"
        val density = getDensity(sft.getTypeName, q)
        density.map(_._3).sum must beGreaterThan(0.0)
      }

      "with spatio-temporal index" >> {
        val q = "BBOX(geom, -78.598118, 37.992204, -78.337364, 38.091238) AND " +
            "dtg between '2012-01-01T18:00:00.000Z' AND '2012-01-01T23:00:00.000Z'"
        val density = getDensity(sft.getTypeName, q)
        density.map(_._3).sum must beGreaterThan(0.0)
      }
    }

    "do density calc on a realistic multilinestring" >> {

      var sft: SimpleFeatureType = null

      "add features" >> {
        sft = createNewSchema(spec("MultiLineString"))
        addFeatures(sft, (0 until 15).toArray.map { i =>
          val sf = new ScalaSimpleFeature(sft, i.toString)
          sf.setAttribute(0, i.toString)
          sf.setAttribute(1, "1.0")
          sf.setAttribute(2, new Date(date + i * 60000))
          sf.setAttribute(3, testData("[MULTILINE] Cherry Avenue entirety"))
          sf
        })
        ok
      }

      "with spatial index" >> {
        val q = "BBOX(geom, -78.511236, 38.019947, -78.485830, 38.030265)"
        val density = getDensity(sft.getTypeName, q)
        density.map(_._3).sum must beGreaterThan(0.0)
      }

      "with spatio-temporal index" >> {
        val q = "BBOX(geom, -78.511236, 38.019947, -78.485830, 38.030265) AND " +
            "dtg between '2012-01-01T18:00:00.000Z' AND '2012-01-01T23:00:00.000Z'"
        val density = getDensity(sft.getTypeName, q)
        density.map(_._3).sum must beGreaterThan(0.0)
      }
    }

    "do density calc on a realistic linestring" >> {

      var sft: SimpleFeatureType = null

      "add features" >> {
        sft = createNewSchema(spec("LineString"))
        addFeatures(sft, (0 until 15).toArray.map { i =>
          val sf = new ScalaSimpleFeature(sft, i.toString)
          sf.setAttribute(0, i.toString)
          sf.setAttribute(1, "1.0")
          sf.setAttribute(2, new Date(date + i * 60000))
          sf.setAttribute(3, testData("[LINE] Cherry Avenue segment"))
          sf
        })
        ok
      }

      "with spatial index" >> {
        val q = "BBOX(geom, -78.511236, 38.019947, -78.485830, 38.030265)"
        val density = getDensity(sft.getTypeName, q)
        density.map(_._3).sum must beGreaterThan(0.0)
      }

      "with spatio-temporal index" >> {
        val q = "BBOX(geom, -78.511236, 38.019947, -78.485830, 38.030265) AND " +
            "dtg between '2012-01-01T18:00:00.000Z' AND '2012-01-01T23:00:00.000Z'"
        val density = getDensity(sft.getTypeName, q)
        density.map(_._3).sum must beGreaterThan(0.0)
      }
    }

    "do density calc on a linestring with multiLine intersect" >> {

      var sft: SimpleFeatureType = null

      "add features" >> {
        sft = createNewSchema(spec("LineString"))
        addFeatures(sft, (0 until 15).toArray.map { i =>
          val sf = new ScalaSimpleFeature(sft, i.toString)
          sf.setAttribute(0, i.toString)
          sf.setAttribute(1, "1.0")
          sf.setAttribute(2, new Date(date + i * 60000))
          sf.setAttribute(3, testData("[LINE] Line to MultiLine segment"))
          sf
        })
        ok
      }

      "with spatial index" >> {
        val q = "BBOX(geom, -78.541236, 38.019947, -78.485830, 38.060265)"
        val density = getDensity(sft.getTypeName, q)
        density.map(_._3).sum must beGreaterThan(0.0)
      }

      "with spatio-temporal index" >> {
        val q = "BBOX(geom, -78.511236, 38.019947, -78.485830, 38.030265) AND " +
          "dtg between '2012-01-01T18:00:00.000Z' AND '2012-01-01T23:00:00.000Z'"
        val density = getDensity(sft.getTypeName, q)
        density.map(_._3).sum must beGreaterThan(0.0)
      }
    }

    "do density calc on a simplistic multi polygon" >> {

      var sft: SimpleFeatureType = null

      "add features" >> {
        sft = createNewSchema(spec("MultiPolygon"))
        addFeatures(sft, (0 until 15).toArray.map { i =>
          val sf = new ScalaSimpleFeature(sft, i.toString)
          sf.setAttribute(0, i.toString)
          sf.setAttribute(1, "1.0")
          sf.setAttribute(2, new Date(date + i * 60000))
          sf.setAttribute(3, testData("[MULTIPOLYGON] test box"))
          sf
        })
        ok
      }

      "with spatial index" >> {
        val q = "BBOX(geom, 0.0, 0.0, 10.0, 10.0)"
        val density = getDensity(sft.getTypeName, q)
        density.map(_._3).sum must beGreaterThan(0.0)
      }

      "with spatio-temporal index" >> {
        val q = "BBOX(geom, -1.0, -1.0, 11.0, 11.0) AND " +
            "dtg between '2012-01-01T18:00:00.000Z' AND '2012-01-01T23:00:00.000Z'"
        val density = getDensity(sft.getTypeName, q)
        density.map(_._3).sum must beGreaterThan(0.0)
      }
    }

    "do density calc on a simplistic linestring" >> {

      var sft: SimpleFeatureType = null

      "add features" >> {
        sft = createNewSchema(spec("LineString"))
        addFeatures(sft, (0 until 15).toArray.map { i =>
          val sf = new ScalaSimpleFeature(sft, i.toString)
          sf.setAttribute(0, i.toString)
          sf.setAttribute(1, "1.0")
          sf.setAttribute(2, new Date(date + i * 60000))
          sf.setAttribute(3, testData("[LINE] test line"))
          sf
        })
        ok
      }

      "with spatial index" >> {
        val q = "BBOX(geom, 0.0, 0.0, 10.0, 10.0)"
        val density = getDensity(sft.getTypeName, q)
        density.map(_._3).sum must beGreaterThan(0.0)
      }

      "with spatio-temporal index" >> {
        val q = "BBOX(geom, -1.0, -1.0, 11.0, 11.0) AND " +
            "dtg between '2012-01-01T18:00:00.000Z' AND '2012-01-01T23:00:00.000Z'"
        val density = getDensity(sft.getTypeName, q)
        density.map(_._3).sum must beGreaterThan(0.0)
      }
    }
  }
}
