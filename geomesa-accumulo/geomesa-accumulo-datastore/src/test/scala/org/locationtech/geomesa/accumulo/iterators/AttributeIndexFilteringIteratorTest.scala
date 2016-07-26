/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
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
import org.locationtech.geomesa.accumulo.index.Strategy.StrategyType
import org.locationtech.geomesa.accumulo.index.Strategy.StrategyType.StrategyType
import org.locationtech.geomesa.accumulo.index._
import org.locationtech.geomesa.accumulo.util.SelfClosingIterator
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class AttributeIndexFilteringIteratorTest extends Specification with TestWithDataStore {

  sequential

  override val spec = s"name:String:index=true,age:Integer:index=true,dtg:Date,*geom:Point:srid=4326"

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

  def checkStrategies[T](query: Query, strategy: StrategyType) = {
    val out = new ExplainString
    val plan = ds.getQueryPlan(query)
    plan must haveLength(1)
    plan.head.filter.strategy mustEqual strategy
  }

  "AttributeIndexFilteringIterator" should {

    "handle like queries and choose correct strategies" in {
      // Try out wildcard queries using the % wildcard syntax.
      // Test single wildcard, trailing, leading, and both trailing & leading wildcards

      // % should return all features
      val wildCardQuery = new Query(sftName, ff.like(ff.property("name"),"%"))
      checkStrategies(wildCardQuery, StrategyType.ATTRIBUTE)
      SelfClosingIterator(fs.getFeatures()) must haveLength(16)

      forall(List("a", "b", "c", "d")) { letter =>
        // 4 features for this letter
        val leftWildCard = new Query(sftName, ff.like(ff.property("name"),s"%$letter"))
        checkStrategies(leftWildCard, StrategyType.Z3)
        SelfClosingIterator(fs.getFeatures(leftWildCard)) must haveLength(4)

        // Double wildcards should be full table scan
        val doubleWildCard = new Query(sftName, ff.like(ff.property("name"),s"%$letter%"))
        checkStrategies(doubleWildCard, StrategyType.Z3)
        SelfClosingIterator(fs.getFeatures(doubleWildCard)) must haveLength(4)

        // should return the 4 features for this letter
        val rightWildcard = new Query(sftName, ff.like(ff.property("name"),s"$letter%"))
        checkStrategies(rightWildcard, StrategyType.ATTRIBUTE)
        SelfClosingIterator(fs.getFeatures(rightWildcard)) must haveLength(4)
      }
    }

    "actually handle transforms properly and chose correct strategies for attribute indexing" in {
      // transform to only return the attribute geom - dropping dtg, age, and name
      val query = new Query(sftName, ECQL.toFilter("name = 'b'"), Array("geom"))
      checkStrategies(query, StrategyType.ATTRIBUTE)

      // full table scan
      val leftWildCard = new Query(sftName, ff.like(ff.property("name"), "%b"), Array("geom"))
      checkStrategies(leftWildCard, StrategyType.Z3)

      // full table scan
      val doubleWildCard = new Query(sftName, ff.like(ff.property("name"), "%b%"), Array("geom"))
      checkStrategies(doubleWildCard, StrategyType.Z3)

      val rightWildcard = new Query(sftName, ff.like(ff.property("name"), "b%"), Array("geom"))
      checkStrategies(rightWildcard, StrategyType.ATTRIBUTE)

      forall(List(query, leftWildCard, doubleWildCard, rightWildcard)) { query =>
        val features = SelfClosingIterator(fs.getFeatures(query)).toList
        features must haveLength(4)
        forall(features)(_.getAttribute(0) must beAnInstanceOf[Geometry])
        forall(features)(_.getAttributeCount mustEqual 1)
      }
    }

    "handle corner case with attr idx, bbox, and no temporal filter" in {
      val filter = ff.and(ECQL.toFilter("name = 'b'"), ECQL.toFilter("BBOX(geom, 30, 30, 50, 50)"))
      val query = new Query(sftName, filter, Array("geom"))
      QueryStrategyDecider.chooseStrategies(sft, query, ds.stats, None).head must
          beAnInstanceOf[Z2IdxStrategy]

      val features = SelfClosingIterator(fs.getFeatures(query)).toList

      features must haveLength(4)
      forall(features)(_.getAttribute(0) must beAnInstanceOf[Geometry])
      forall(features)(_.getAttributeCount mustEqual 1)
    }
  }

}
