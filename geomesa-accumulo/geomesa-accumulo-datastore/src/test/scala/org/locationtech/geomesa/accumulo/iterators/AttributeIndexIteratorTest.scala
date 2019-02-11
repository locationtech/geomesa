/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import java.text.SimpleDateFormat
import java.util.{Collections, Date, TimeZone}

import org.geotools.data.Query
import org.geotools.factory.Hints
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo._
import org.locationtech.geomesa.accumulo.index.JoinIndex
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.index.conf.QueryHints.QUERY_INDEX
import org.locationtech.geomesa.index.index.z2.Z2Index
import org.locationtech.geomesa.index.utils.{ExplainNull, Explainer}
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class AttributeIndexIteratorTest extends Specification with TestWithDataStore {

  sequential

  val spec = "name:String:index=join,age:Integer:index=join,scars:List[String]:index=join,dtg:Date:index=join," +
      "*geom:Point:srid=4326;override.index.dtg.join=true"

  val dateToIndex = {
    val sdf = new SimpleDateFormat("yyyyMMdd")
    sdf.setTimeZone(TimeZone.getTimeZone("Zulu"))
    sdf.parse("20140102")
  }

  addFeatures({
    List("a", "b", "c", "d", null).flatMap { name =>
      List(1, 2, 3, 4).zip(List(45, 46, 47, 48)).map { case (i, lat) =>
        val sf = SimpleFeatureBuilder.build(sft, List(), name + i.toString)
        sf.setDefaultGeometry(WKTUtils.read(f"POINT($lat%d $lat%d)"))
        sf.setAttribute("dtg", dateToIndex)
        sf.setAttribute("age", i)
        sf.setAttribute("name", name)
        sf.setAttribute("scars", Collections.singletonList("face"))
        sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
        sf
      }
    }
  })

  val queryPlanner = ds.queryPlanner

  def query(filter: String, attributes: Array[String] = Array.empty, explain: Explainer = ExplainNull) = {
    val query = new Query(sftName, ECQL.toFilter(filter), if (attributes.length == 0) null else attributes)
    query.getHints.put(QUERY_INDEX, JoinIndex.name)
    queryPlanner.runQuery(sft, query, explain).toList
  }

  "AttributeIndexIterator" should {

    "return correct results" >> {

      "for string equals" >> {
        val filter = "name = 'b'"
        val results = query(filter, Array("geom", "dtg", "name"))

        results must haveSize(4)
        results.map(_.getAttributeCount) must contain(3).foreach
        foreach(results.map(_.getAttribute("name").asInstanceOf[String]))(_ must contain("b"))
        results.map(_.getAttribute("geom").toString) must contain("POINT (45 45)", "POINT (46 46)", "POINT (47 47)", "POINT (48 48)")
        results.map(_.getAttribute("dtg").asInstanceOf[Date]) must contain(dateToIndex).foreach
      }

      "for string less than" >> {
        val filter = "name < 'b'"
        val results = query(filter, Array("geom", "dtg", "name"))

        results must haveSize(4)
        results.map(_.getAttributeCount) must contain(3).foreach
        foreach(results.map(_.getAttribute("name").asInstanceOf[String]))(_ must contain("a"))
        results.map(_.getAttribute("geom").toString) must contain("POINT (45 45)", "POINT (46 46)", "POINT (47 47)", "POINT (48 48)")
        results.map(_.getAttribute("dtg").asInstanceOf[Date]) must contain(dateToIndex).foreach
      }

      "for string greater than" >> {
        val filter = "name > 'b'"
        val results = query(filter, Array("geom", "dtg", "name"))

        results must haveSize(8)
        results.map(_.getAttributeCount) must contain(3).foreach
        results.map(_.getAttribute("name").asInstanceOf[String]) must contain(beEqualTo("c")).exactly(4)
        results.map(_.getAttribute("name").asInstanceOf[String]) must contain(beEqualTo("d")).exactly(4)
        results.map(_.getAttribute("geom").toString) must contain("POINT (45 45)", "POINT (46 46)", "POINT (47 47)", "POINT (48 48)")
        results.map(_.getAttribute("dtg").asInstanceOf[Date]) must contain(dateToIndex).foreach
      }

      "for string greater than or equals" >> {
        val filter = "name >= 'b'"
        val results = query(filter, Array("geom", "dtg", "name"))

        results must haveSize(12)
        results.map(_.getAttributeCount) must contain(3).foreach
        results.map(_.getAttribute("name").asInstanceOf[String]) must contain(beEqualTo("b")).exactly(4)
        results.map(_.getAttribute("name").asInstanceOf[String]) must contain(beEqualTo("c")).exactly(4)
        results.map(_.getAttribute("name").asInstanceOf[String]) must contain(beEqualTo("d")).exactly(4)
        results.map(_.getAttribute("geom").toString) must contain("POINT (45 45)", "POINT (46 46)", "POINT (47 47)", "POINT (48 48)")
        results.map(_.getAttribute("dtg").asInstanceOf[Date]) must contain(dateToIndex).foreach
      }

      "for date tequals" >> {
        val filter = "dtg TEQUALS 2014-01-02T00:00:00.000Z"
        val results = query(filter, Array("geom", "dtg"))

        results must haveSize(20)
        results.map(_.getAttributeCount) must contain(2).foreach
      }

      "for date equals" >> {
        val filter = "dtg = '2014-01-02T00:00:00.000Z'"
        val results = query(filter, Array("geom", "dtg"))

        results must haveSize(20)
        results.map(_.getAttributeCount) must contain(2).foreach
      }

      "for date between" >> {
        val filter = "dtg BETWEEN '2014-01-01T00:00:00.000Z' AND '2014-01-03T00:00:00.000Z'"
        val results = query(filter, Array("geom", "dtg"))

        results must haveSize(20)
        results.map(_.getAttributeCount) must contain(2).foreach
      }

      "for int less than" >> {
        val filter = "age < 2"
        val results = query(filter, Array("geom", "dtg", "age"))

        results must haveSize(5)
        results.map(_.getAttributeCount) must contain(3).foreach
        results.map(_.getAttribute("age").asInstanceOf[Int]) must contain(1).foreach
        foreach(results.map(_.getAttribute("geom").toString))(_ must contain("POINT (45 45)"))
        results.map(_.getAttribute("dtg").asInstanceOf[Date]) must contain(dateToIndex).foreach
      }

      "for int greater than or equals" >> {
        val filter = "age >= 3"
        val results = query(filter, Array("geom", "dtg", "age"))

        results must haveSize(10)
        results.map(_.getAttributeCount) must contain(3).foreach
        results.map(_.getAttribute("age").asInstanceOf[Int]) must contain(3).exactly(5)
        results.map(_.getAttribute("age").asInstanceOf[Int]) must contain(4).exactly(5)
        results.map(_.getAttribute("geom").toString) must contain(beEqualTo("POINT (47 47)")).exactly(5)
        results.map(_.getAttribute("geom").toString) must contain(beEqualTo("POINT (48 48)")).exactly(5)
        results.map(_.getAttribute("dtg").asInstanceOf[Date]) must contain(dateToIndex).foreach
      }

      "not including attribute queried on" >> {
        val filter = "name = 'b'"
        val results = query(filter, Array("geom", "dtg"))

        results must haveSize(4)
        results.map(_.getAttributeCount) must contain(2).foreach
        results.map(_.getAttribute("geom").toString) must contain("POINT (45 45)", "POINT (46 46)", "POINT (47 47)", "POINT (48 48)")
        forall(results.map(_.getAttribute("dtg").asInstanceOf[Date]))(_ mustEqual dateToIndex)
      }

      "not including geom" >> {
        val filter = "name = 'b'"
        val results = query(filter, Array("dtg"))

        results must haveSize(4)
        results.map(_.getAttributeCount) must contain(1).foreach
        results.map(_.getAttribute("dtg").asInstanceOf[Date]) must contain(dateToIndex).foreach
      }

      "not including dtg" >> {
        val filter = "name = 'b'"
        val results = query(filter, Array("geom"))

        results must haveSize(4)
        results.map(_.getAttributeCount) must contain(1).foreach
        results.map(_.getAttribute("geom").toString) must contain("POINT (45 45)", "POINT (46 46)", "POINT (47 47)", "POINT (48 48)")
      }

      "not including geom or dtg" >> {
        val filter = "name = 'b'"
        val results = query(filter, Array("name"))

        results must haveSize(4)
        results.map(_.getAttributeCount) must contain(1).foreach
        foreach(results.map(_.getAttribute("name").toString))(_ must contain("b"))
      }

      "with additional filter applied" >> {
        val filter = "name = 'b' AND BBOX(geom, 44.5, 44.5, 45.5, 45.5)"
        val results = query(filter, Array("geom", "dtg", "name"))

        results must haveSize(1)
        results.map(_.getAttributeCount) must contain(3).foreach // geom gets added back in
        results.map(_.getAttribute("name").toString) must contain("b")
        results.map(_.getAttribute("geom").toString) must contain("POINT (45 45)")
      }
    }
  }
}
