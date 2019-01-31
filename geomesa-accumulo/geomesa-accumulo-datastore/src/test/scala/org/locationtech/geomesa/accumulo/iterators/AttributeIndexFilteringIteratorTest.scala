/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import org.locationtech.jts.geom.Geometry
import org.geotools.data.Query
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.accumulo.index.JoinIndex
import org.locationtech.geomesa.index.index.NamedIndex
import org.locationtech.geomesa.index.index.z3.Z3Index
import org.locationtech.geomesa.index.utils.{ExplainNull, Explainer}
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class AttributeIndexFilteringIteratorTest extends Specification with TestWithDataStore {

  import org.locationtech.geomesa.filter.ff

  sequential

  override val spec = s"name:String:index=join,age:Integer:index=join,dtg:Date,*geom:Point:srid=4326"

  val features = List("a", "b", "c", "d").flatMap { name =>
    List(1, 2, 3, 4).zip(List(45, 46, 47, 48)).map { case (i, lat) =>
      val sf = SimpleFeatureBuilder.build(sft, List(), name + i.toString)
      sf.setDefaultGeometry(WKTUtils.read(f"POINT($lat%d $lat%d)"))
      sf.setAttribute("dtg", "2011-01-01T00:00:00Z")
      sf.setAttribute("age", i)
      sf.setAttribute("name", name)
      sf
    }
  }

  addFeatures(features)

  def checkStrategies[T](query: Query, strategy: NamedIndex, explain: Explainer = ExplainNull): MatchResult[Any] = {
    val plan = ds.getQueryPlan(query, explainer = explain)
    plan must haveLength(1)
    plan.head.filter.index.name mustEqual strategy.name
  }

  "AttributeIndexFilteringIterator" should {

    "handle like queries and choose correct strategies" in {
      // Try out wildcard queries using the % wildcard syntax.
      // Test single wildcard, trailing, leading, and both trailing & leading wildcards

      // % should return all features
      val wildCardQuery = new Query(sftName, ff.like(ff.property("name"),"%"))
      checkStrategies(wildCardQuery, JoinIndex)
      SelfClosingIterator(fs.getFeatures(wildCardQuery)).toSeq must haveLength(16)

      forall(List("a", "b", "c", "d")) { letter =>
        // 4 features for this letter
        val leftWildCard = new Query(sftName, ff.like(ff.property("name"),s"%$letter"))
        checkStrategies(leftWildCard, Z3Index)
        SelfClosingIterator(fs.getFeatures(leftWildCard)).toSeq must haveLength(4)

        // Double wildcards should be full table scan
        val doubleWildCard = new Query(sftName, ff.like(ff.property("name"),s"%$letter%"))
        checkStrategies(doubleWildCard, Z3Index)
        SelfClosingIterator(fs.getFeatures(doubleWildCard)).toSeq must haveLength(4)

        // should return the 4 features for this letter
        val rightWildcard = new Query(sftName, ff.like(ff.property("name"),s"$letter%"))
        checkStrategies(rightWildcard, JoinIndex)
        SelfClosingIterator(fs.getFeatures(rightWildcard)).toSeq must haveLength(4)
      }
    }

    "actually handle transforms properly and chose correct strategies for attribute indexing" in {
      // transform to only return the attribute geom - dropping dtg, age, and name
      val query = new Query(sftName, ECQL.toFilter("name = 'b'"), Array("geom"))
      checkStrategies(query, JoinIndex)

      // full table scan
      val leftWildCard = new Query(sftName, ff.like(ff.property("name"), "%b"), Array("geom"))
      checkStrategies(leftWildCard, Z3Index)

      // full table scan
      val doubleWildCard = new Query(sftName, ff.like(ff.property("name"), "%b%"), Array("geom"))
      checkStrategies(doubleWildCard, Z3Index)

      val rightWildcard = new Query(sftName, ff.like(ff.property("name"), "b%"), Array("geom"))
      checkStrategies(rightWildcard, JoinIndex)

      forall(List(query, leftWildCard, doubleWildCard, rightWildcard)) { query =>
        val features = SelfClosingIterator(fs.getFeatures(query)).toList
        features must haveLength(4)
        forall(features)(_.getAttribute(0) must beAnInstanceOf[Geometry])
        forall(features)(_.getAttributeCount mustEqual 1)
      }
    }

    "handle corner case with attr idx, bbox, and no temporal filter" in {
      ds.stats.generateStats(sft)
      val filter = ff.and(ECQL.toFilter("name = 'b'"), ECQL.toFilter("BBOX(geom, 30, 30, 50, 50)"))
      val query = new Query(sftName, filter, Array("geom"))
      ds.getQueryPlan(query).head.filter.index.name mustEqual JoinIndex.name

      val features = SelfClosingIterator(fs.getFeatures(query)).toList

      features must haveLength(4)
      forall(features)(_.getAttribute(0) must beAnInstanceOf[Geometry])
      forall(features)(_.getAttributeCount mustEqual 1)
    }
  }

}
