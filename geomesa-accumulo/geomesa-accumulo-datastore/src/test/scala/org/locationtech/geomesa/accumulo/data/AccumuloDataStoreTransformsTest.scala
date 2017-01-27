/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.data

import java.util.Date

import com.vividsolutions.jts.geom.Point
import org.geotools.data._
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.text.cql2.CQL
import org.geotools.filter.text.ecql.ECQL
import org.geotools.util.Converters
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithMultipleSfts
import org.locationtech.geomesa.accumulo.util.{CloseableIterator, SelfClosingIterator}
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AccumuloDataStoreTransformsTest extends Specification with TestWithMultipleSfts {

  sequential

  val spec  = "name:String,dtg:Date,*geom:Point:srid=4326"
  val spec2 = "name:String,attr:String,dtg:Date,*geom:Point:srid=4326"

  val name = "myname"
  val date = Converters.convert("2012-01-01T00:00:00.000Z", classOf[Date])
  val geom = Converters.convert("POINT(45 49)", classOf[Point])

  val ff = CommonFactoryFinder.getFilterFactory2

  def createFeature(sft: SimpleFeatureType) =
    Seq(new ScalaSimpleFeature("fid-1", sft, Array(name, date, geom)))
  def createFeature2(sft: SimpleFeatureType, attr: String) =
    Seq(new ScalaSimpleFeature("fid-1", sft, Array(name, attr, date, geom)))

  "AccumuloDataStore" should {

    "handle transformations" >> {
      val sft = createNewSchema(spec)
      val sftName = sft.getTypeName
      addFeatures(sft, createFeature(sft))

      "with derived values" >> {
        val query = new Query(sftName, Filter.INCLUDE,
          Array("name", "derived=strConcat('hello',name)", "geom"))

        // Let's read out what we wrote.
        val results = ds.getFeatureSource(sftName).getFeatures(query)

        "with the correct schema" >> {
          val schema = SimpleFeatureTypes.encodeType(results.getSchema)
          schema mustEqual "name:String,*geom:Point:srid=4326,derived:String"
        }
        "with the correct results" >> {
          val features = results.features
          features.hasNext must beTrue
          val f = features.next()
          DataUtilities.encodeFeature(f) mustEqual "fid-1=myname|POINT (45 49)|hellomyname"
        }
      }

      "with dtg and geom" in {
        val query = new Query(sftName, Filter.INCLUDE, List("dtg", "geom").toArray)
        val results = SelfClosingIterator(CloseableIterator(ds.getFeatureSource(sftName).getFeatures(query).features())).toList
        results must haveSize(1)
        results.head.getID mustEqual "fid-1"
        results.head.getAttribute("dtg") mustEqual date
        results.head.getAttribute("geom") mustEqual geom
        results.head.getAttribute("name") must beNull
      }

      "with setPropertyNames" in {
        val filter = ff.bbox("geom", 44.0, 48.0, 46.0, 50.0, "EPSG:4326")
        val query = new Query(sftName, filter)
        query.setPropertyNames(Array("geom"))

        val features = ds.getFeatureSource(sftName).getFeatures(query).features

        val results = features.toList

        "return exactly one result" >> {
          results.size  must equalTo(1)
        }
        "with correct fields" >> {
          results.head.getID mustEqual "fid-1"
          results.head.getAttribute("geom") mustEqual geom
          results.head.getAttribute("dtg") must beNull
          results.head.getAttribute("name") must beNull
        }
      }

      "with renaming projections" in {
        val query = new Query(sftName, Filter.INCLUDE, Array("trans=name", "geom"))

        val features = ds.getFeatureSource(sftName).getFeatures(query).features().toList

        features must haveSize(1)
        features.head.getID mustEqual "fid-1"
        features.head.getAttributeCount mustEqual 2
        features.head.getAttribute("trans") mustEqual name
        features.head.getAttribute("geom") mustEqual geom
      }
    }

    "handle transformations" >> {
      val sft = createNewSchema(spec2)
      val sftName = sft.getTypeName
      addFeatures(sft, createFeature2(sft, "v1"))

      "across multiple fields" >> {
        val query = new Query(sftName, Filter.INCLUDE,
          Array("name", "derived=strConcat(attr,name)", "geom"))

        // Let's read out what we wrote.
        val results = ds.getFeatureSource(sftName).getFeatures(query)

        "with the correct schema" >> {
          SimpleFeatureTypes.encodeType(results.getSchema) mustEqual "name:String,*geom:Point:srid=4326,derived:String"
        }
        "with the correct results" >> {
          val features = results.features
          features.hasNext must beTrue
          val f = features.next()
          DataUtilities.encodeFeature(f) mustEqual "fid-1=myname|POINT (45 49)|v1myname"
        }
      }

      "to subtypes" >> {
        val query = new Query(sftName, Filter.INCLUDE, Array("name", "geom"))

        // Let's read out what we wrote.
        val results = ds.getFeatureSource(sftName).getFeatures(query)

        "with the correct schema" >> {
          SimpleFeatureTypes.encodeType(results.getSchema) mustEqual "name:String,*geom:Point:srid=4326"
        }
        "with the correct results" >> {
          val features = results.features
          features.hasNext must beTrue
          val f = features.next()
          DataUtilities.encodeFeature(f) mustEqual "fid-1=myname|POINT (45 49)"
        }
      }

      "with filters on other attributes" >> {
        val filter = CQL.toFilter("bbox(geom,45,45,55,55) AND " +
            "dtg BETWEEN '2011-12-31T00:00:00.000Z' AND '2012-01-02T00:00:00.000Z'")
        val query = new Query(sftName, filter, Array("geom"))

        // Let's read out what we wrote.
        val features = ds.getFeatureSource(sftName).getFeatures(query).features
        "return the data" >> {
          features.hasNext must beTrue
        }
        "with correct results" >> {
          val f = features.next()
          DataUtilities.encodeFeature(f) mustEqual "fid-1=POINT (45 49)"
        }
      }
    }

    "transform index value data correctly" in {
      val sft = createNewSchema("trackId:String:index-value=true,label:String:index-value=true," +
          "extraValue:String,score:Double:index-value=true,dtg:Date,geom:Point:srid=4326")
      val sftName = sft.getTypeName

      val baseDate = Converters.convert("2014-01-01T00:00:00.000Z", classOf[Date]).getTime

      addFeatures(sft, {
        (0 until 5).map { i =>
          val sf = new ScalaSimpleFeature(s"f$i", sft)
          sf.setAttribute(0, s"trk$i")
          sf.setAttribute(1, s"label$i")
          sf.setAttribute(2, "extra")
          sf.setAttribute(3, new java.lang.Double(i))
          sf.setAttribute(4, s"2014-01-01T0$i:00:00.000Z")
          sf.setAttribute(5, s"POINT(5$i 50)")
          sf
        }
      })

      "with out of order attributes" >> {
        val query = new Query(sftName, ECQL.toFilter("bbox(geom,49,49,60,60)"), Array("geom", "dtg", "label"))
        val features =
          SelfClosingIterator(ds.getFeatureSource(sftName).getFeatures(query).features).toList.sortBy(_.getID)
        features must haveSize(5)
        (0 until 5).foreach { i =>
          features(i).getID mustEqual s"f$i"
          features(i).getAttributeCount mustEqual 3
          features(i).getAttribute("label") mustEqual s"label$i"
          features(i).getAttribute("dtg").asInstanceOf[Date].getTime mustEqual baseDate + i * 60 *60 * 1000
          features(i).getAttribute("geom") mustEqual WKTUtils.read(s"POINT(5$i 50)")
        }
        success
      }

      "with only date and geom" >> {
        val query = new Query(sftName, ECQL.toFilter("bbox(geom,49,49,60,60)"), Array("geom", "dtg"))
        val features =
          SelfClosingIterator(ds.getFeatureSource(sftName).getFeatures(query).features).toList.sortBy(_.getID)
        features must haveSize(5)
        (0 until 5).foreach { i =>
          features(i).getID mustEqual s"f$i"
          features(i).getAttributeCount mustEqual 2
          features(i).getAttribute("dtg").asInstanceOf[Date].getTime mustEqual baseDate + i * 60 *60 * 1000
          features(i).getAttribute("geom") mustEqual WKTUtils.read(s"POINT(5$i 50)")
        }
        success
      }

      "with all attributes" >> {
        val query = new Query(sftName, ECQL.toFilter("bbox(geom,49,49,60,60)"),
          Array("geom", "dtg", "label", "score", "trackId"))
        val features =
          SelfClosingIterator(ds.getFeatureSource(sftName).getFeatures(query).features).toList.sortBy(_.getID)
        features must haveSize(5)
        (0 until 5).foreach { i =>
          features(i).getID mustEqual s"f$i"
          features(i).getAttributeCount mustEqual 5
          features(i).getAttribute("label") mustEqual s"label$i"
          features(i).getAttribute("trackId") mustEqual s"trk$i"
          features(i).getAttribute("score") mustEqual i.toDouble
          features(i).getAttribute("dtg").asInstanceOf[Date].getTime mustEqual baseDate + i * 60 *60 * 1000
          features(i).getAttribute("geom") mustEqual WKTUtils.read(s"POINT(5$i 50)")
        }
        success
      }
    }

    "always return a geometry if available" >> {
      val sft = createNewSchema("name:String,dtg:Date,*geom:Point:srid=4326,geom2:Point:srid=4326")
      val sftName = sft.getTypeName

      val reference = (0 until 3).map { i =>
        val sf = new ScalaSimpleFeature(s"f$i", sft)
        sf.setAttribute(0, s"name$i")
        sf.setAttribute(1, s"2014-01-01T0$i:00:00.000Z")
        sf.setAttribute(2, s"POINT(5$i 50)")
        sf.setAttribute(3, s"POINT(6$i 50)")
        sf
      }
      addFeatures(sft, reference)

      "if no geometry is specified" >> {
        val query = new Query(sftName, Filter.INCLUDE, Array("name"))
        val features = SelfClosingIterator(ds.getFeatureSource(sftName).getFeatures(query).features).toSeq
        features must haveSize(3)
        forall(features)(_.getAttributeCount mustEqual 2)
        features.map(_.getAttribute("geom")) must containTheSameElementsAs(reference.map(_.getAttribute("geom")))
        features.map(_.getAttribute("name")) must containTheSameElementsAs(reference.map(_.getAttribute("name")))
      }

      "if default geometry is specified" >> {
        val query = new Query(sftName, Filter.INCLUDE, Array("name", "geom"))
        val features = SelfClosingIterator(ds.getFeatureSource(sftName).getFeatures(query).features).toSeq
        features must haveSize(3)
        forall(features)(_.getAttributeCount mustEqual 2)
        features.map(_.getAttribute("geom")) must containTheSameElementsAs(reference.map(_.getAttribute("geom")))
        features.map(_.getAttribute("name")) must containTheSameElementsAs(reference.map(_.getAttribute("name")))
      }

      "if alternate geometry is specified" >> {
        val query = new Query(sftName, Filter.INCLUDE, Array("name", "geom2"))
        val features = SelfClosingIterator(ds.getFeatureSource(sftName).getFeatures(query).features).toSeq
        features must haveSize(3)
        forall(features)(_.getAttributeCount mustEqual 2)
        features.map(_.getAttribute("geom2")) must containTheSameElementsAs(reference.map(_.getAttribute("geom2")))
        features.map(_.getAttribute("name")) must containTheSameElementsAs(reference.map(_.getAttribute("name")))
      }
    }
  }
}
