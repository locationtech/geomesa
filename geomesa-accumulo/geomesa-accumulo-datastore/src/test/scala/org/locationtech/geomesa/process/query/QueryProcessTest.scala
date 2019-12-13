/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.process.query

import org.locationtech.jts.geom.Geometry
import org.geotools.filter.text.cql2.CQL
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureFactory
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class QueryProcessTest extends Specification with TestWithDataStore {

  sequential

  override val spec = "type:String,*geom:Point:srid=4326,dtg:Date"

  val features = List("a", "b").flatMap { name =>
    List(1, 2, 3, 4).zip(List(45, 46, 47, 48)).map { case (i, lat) =>
      val sf = AvroSimpleFeatureFactory.buildAvroFeature(sft, List(), name + i.toString)
      sf.setDefaultGeometry(WKTUtils.read(f"POINT($lat%d $lat%d)"))
      sf.setAttribute("dtg", s"2011-01-0${i}T00:00:00Z")
      sf.setAttribute("type", name)
      sf
    }
  }

  addFeatures(features)

  "GeomesaQuery" should {
    "return things without a filter" in {
      val features = fs.getFeatures()

      val geomesaQuery = new QueryProcess
      val results = geomesaQuery.execute(features, null)

      val f = SelfClosingIterator(results).toList
      forall(f)(_.getAttribute("type") must beOneOf("a", "b"))
      f must haveLength(8)
    }

    "respect a parent filter" in {
      val features = fs.getFeatures(CQL.toFilter("type = 'b'"))

      val geomesaQuery = new QueryProcess
      val results = geomesaQuery.execute(features, null)

      val f = SelfClosingIterator(results).toList
      forall(f)(_.getAttribute("type") mustEqual "b")
      f must haveLength(4)
    }

    "be able to use its own filter" in {
      val features = fs.getFeatures(CQL.toFilter("type = 'b' OR type = 'a'"))

      val geomesaQuery = new QueryProcess
      val results = geomesaQuery.execute(features, CQL.toFilter("type = 'a'"))

      val f = SelfClosingIterator(results).toList
      forall(f)(_.getAttribute("type") mustEqual "a")
      f must haveLength(4)
    }

    "query wtih short dates (using joda conversion factory)" in {
      val features = fs.getFeatures()

      val geomesaQuery = new QueryProcess
      val results = geomesaQuery.execute(features, CQL.toFilter("dtg between '2011-01-01' AND '2011-01-02'"))

      val f = SelfClosingIterator(results.features()).toList

      f must haveLength(4)
    }

    "query with a variety of date formats (using joda conversion factory)" in {
      val features = fs.getFeatures()

      val geomesaQuery = new QueryProcess

      val times = Seq("T00:00", "T00:00:00", "T00:00:00.000").flatMap(time => Seq(time, s"${time}Z")) ++ Seq("")
      val filters = times.map(time => ECQL.toFilter(s"dtg between '2011-01-01$time' AND '2011-01-02$time'"))

      forall(filters)(filter => SelfClosingIterator(geomesaQuery.execute(features, filter)).toList must haveLength(4))
    }

    "properly query geometry" in {
      val features = fs.getFeatures()

      val geomesaQuery = new QueryProcess
      val results = geomesaQuery.execute(features, CQL.toFilter("bbox(geom, 45.0, 45.0, 46.0, 46.0)"))

      val poly = WKTUtils.read("POLYGON((45 45, 46 45, 46 46, 45 46, 45 45))")

      val f = SelfClosingIterator(results).toList
      forall(f)(_.getDefaultGeometry.asInstanceOf[Geometry].intersects(poly) must beTrue)
      f must haveLength(4)
    }

    "allow for projections in the returned result set" in {
      import scala.collection.JavaConversions._

      val features = fs.getFeatures()

      val geomesaQuery = new QueryProcess
      val results = geomesaQuery.execute(features, null, List("type", "geom"))

      val f = SelfClosingIterator(results).toList
      f.head.getType.getAttributeCount mustEqual 2

      forall(f)(_.getAttribute("type") must beOneOf("a", "b"))
      f must haveLength(8)
    }

    "support transforms in the returned result set" in {
      import scala.collection.JavaConversions._

      val features = fs.getFeatures()

      val geomesaQuery = new QueryProcess
      val results = geomesaQuery.execute(features, null, List("type", "geom", "derived=strConcat(type, 'b')"))

      val f = SelfClosingIterator(results).toList
      f.head.getType.getAttributeCount mustEqual 3

      forall(f)(_.getAttribute("type") must beOneOf("a", "b"))
      forall(f)(_.getAttribute("derived") must beOneOf("ab", "bb"))
      f must haveLength(8)
    }

    // NB: We 'filter' and then 'transform'.  Any filter on a 'derived' field must be expressed as a function.
    "support transforms with filters in the returned result set" in {
      import scala.collection.JavaConversions._

      val features = fs.getFeatures()

      val geomesaQuery = new QueryProcess
      val results = geomesaQuery.execute(features,
                                         ECQL.toFilter("strConcat(type, 'b') = 'ab'"),
                                         List("type", "geom", "derived=strConcat(type, 'b')"))

      val f = SelfClosingIterator(results).toList
      f.head.getType.getAttributeCount mustEqual 3

      forall(f)(_.getAttribute("type") mustEqual "a")
      forall(f)(_.getAttribute("derived") mustEqual "ab")
      f must haveLength(4)
    }
  }
}
