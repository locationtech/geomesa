/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import com.vividsolutions.jts.geom.Geometry
import org.geotools.data.Query
import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.ecql.ECQL
import org.joda.time.{DateTime, DateTimeZone}
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.accumulo.index._
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class AttributeIndexFilteringIteratorTest extends Specification with TestWithDataStore {

  sequential

  override val spec = s"name:String:index=true,age:Integer:index=true,dtg:Date,*geom:Geometry:srid=4326"

  val features = List("a", "b", "c", "d").flatMap { name =>
    List(1, 2, 3, 4).zip(List(45, 46, 47, 48)).map { case (i, lat) =>
      val sf = SimpleFeatureBuilder.build(sft, List(), name + i.toString)
      sf.setDefaultGeometry(WKTUtils.read(f"POINT($lat%d $lat%d)"))
      sf.setAttribute("dtg", new DateTime("2011-01-01T00:00:00Z", DateTimeZone.UTC).toDate)
      sf.setAttribute("age", i)
      sf.setAttribute("name", name)
      sf
    }
  }

  addFeatures(features)

  val ff = CommonFactoryFinder.getFilterFactory2

  val hints = new UserDataStrategyHints()

  def checkStrategies[T](query: Query, clas: Class[T]) = {
    val out = new ExplainString
    ds.explainQuery(query, out)
    val lines = out.toString().split("\n").map(_.trim).filter(_.startsWith("Strategy 1 of 1:"))
    lines must haveLength(1)
    lines.head must contain(clas.getSimpleName)
  }


  "AttributeIndexFilteringIterator" should {

    "handle like queries and choose correct strategies" in {
      // Try out wildcard queries using the % wildcard syntax.
      // Test single wildcard, trailing, leading, and both trailing & leading wildcards

      // % should return all features
      val wildCardQuery = new Query(sftName, ff.like(ff.property("name"),"%"))
      checkStrategies(wildCardQuery, classOf[AttributeIdxStrategy])
      fs.getFeatures().features.size mustEqual 16

      forall(List("a", "b", "c", "d")) { letter =>
        // 4 features for this letter
        val leftWildCard = new Query(sftName, ff.like(ff.property("name"),s"%$letter"))
        checkStrategies(leftWildCard, classOf[RecordIdxStrategy])
        fs.getFeatures(leftWildCard).features.size mustEqual 4

        // Double wildcards should be record
        val doubleWildCard = new Query(sftName, ff.like(ff.property("name"),s"%$letter%"))
        checkStrategies(doubleWildCard, classOf[RecordIdxStrategy])
        fs.getFeatures(doubleWildCard).features.size mustEqual 4

        // should return the 4 features for this letter
        val rightWildcard = new Query(sftName, ff.like(ff.property("name"),s"$letter%"))
        checkStrategies(rightWildcard, classOf[AttributeIdxStrategy])
        fs.getFeatures(rightWildcard).features.size mustEqual 4
      }
    }

    "actually handle transforms properly and chose correct strategies for attribute indexing" in {
      // transform to only return the attribute geom - dropping dtg, age, and name
      val query = new Query(sftName, ECQL.toFilter("name = 'b'"), Array("geom"))
      checkStrategies(query, classOf[AttributeIdxStrategy])

      val leftWildCard = new Query(sftName, ff.like(ff.property("name"), "%b"), Array("geom"))
      checkStrategies(leftWildCard, classOf[RecordIdxStrategy])

      val doubleWildCard = new Query(sftName, ff.like(ff.property("name"), "%b%"), Array("geom"))
      checkStrategies(doubleWildCard, classOf[RecordIdxStrategy])

      val rightWildcard = new Query(sftName, ff.like(ff.property("name"), "b%"), Array("geom"))
      checkStrategies(rightWildcard, classOf[AttributeIdxStrategy])

      forall(List(query, leftWildCard, doubleWildCard, rightWildcard)) { query =>
        val features = fs.getFeatures(query)

        features.size mustEqual 4
        forall(features.features) { sf =>
          sf.getAttribute(0) must beAnInstanceOf[Geometry]
        }

        forall(features.features) { sf =>
          sf.getAttributeCount mustEqual 1
        }
        success
      }
    }

    "handle corner case with attr idx, bbox, and no temporal filter" in {
      val filter = ff.and(ECQL.toFilter("name = 'b'"), ECQL.toFilter("BBOX(geom, 30, 30, 50, 50)"))
      val query = new Query(sftName, filter, Array("geom"))
      QueryStrategyDecider.chooseStrategies(sft, query, hints, None).head must
          beAnInstanceOf[STIdxStrategy]

      val features = fs.getFeatures(query)

      features.size mustEqual 4
      forall(features.features) { sf =>
        sf.getAttribute(0) must beAnInstanceOf[Geometry]
      }

      forall(features.features) { sf =>
        sf.getAttributeCount mustEqual 1
      }
    }
  }

}
