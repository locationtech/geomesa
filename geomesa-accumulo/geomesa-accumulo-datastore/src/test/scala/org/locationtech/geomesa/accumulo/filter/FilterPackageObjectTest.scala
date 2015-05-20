/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.accumulo.filter

import com.typesafe.scalalogging.slf4j.Logging
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.filter.TestFilters._
import org.locationtech.geomesa.accumulo.iterators.TestData._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.filter._
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.Fragments

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class FilterPackageObjectTest extends Specification with Logging {

  "The partitionGeom function" should {
    val sft = SimpleFeatureTypes.createType("filterPackageTest", "g:Geometry,*geom:Geometry")

    "filter bbox based on default geom" in {
      val filter = ECQL.toFilter("BBOX(geom, -45.0,-45.0,45.0,45.0) AND BBOX(g, -30.0,-30.0,30.0,30.0)")
      val (geoms, nongeoms) = partitionGeom(filter, sft)
      geoms must haveLength(1)
      nongeoms must haveLength(1)
      ECQL.toCQL(geoms(0)) mustEqual("BBOX(geom, -45.0,-45.0,45.0,45.0)")
      ECQL.toCQL(nongeoms(0)) mustEqual("BBOX(g, -30.0,-30.0,30.0,30.0)")
    }
    "filter intersect based on default geom" in {
      val filter =
        ECQL.toFilter("INTERSECTS(geom, POLYGON ((-45 -45, -45 45, 45 45, 45 -45, -45 -45))) " +
          "AND INTERSECTS(g, POLYGON ((-30 -30, -30 30, 30 30, 30 -30, -30 -30)))")
      val (geoms, nongeoms) = partitionGeom(filter, sft)
      geoms must haveLength(1)
      nongeoms must haveLength(1)
      ECQL.toCQL(geoms(0)) mustEqual("INTERSECTS(geom, POLYGON ((-45 -45, -45 45, 45 45, 45 -45, -45 -45)))")
      ECQL.toCQL(nongeoms(0)) mustEqual("INTERSECTS(g, POLYGON ((-30 -30, -30 30, 30 30, 30 -30, -30 -30)))")
    }
    "filter intersect based on default geom regardless of order" in {
      val filter =
        ECQL.toFilter("INTERSECTS(POLYGON ((-30 -30, -30 30, 30 30, 30 -30, -30 -30)), g) " +
            "AND INTERSECTS(POLYGON ((-45 -45, -45 45, 45 45, 45 -45, -45 -45)), geom)")
      val (geoms, nongeoms) = partitionGeom(filter, sft)
      geoms must haveLength(1)
      nongeoms must haveLength(1)
      ECQL.toCQL(geoms(0)) mustEqual("INTERSECTS(POLYGON ((-45 -45, -45 45, 45 45, 45 -45, -45 -45)), geom)")
      ECQL.toCQL(nongeoms(0)) mustEqual("INTERSECTS(POLYGON ((-30 -30, -30 30, 30 30, 30 -30, -30 -30)), g)")
    }
  }

  "The deMorgan function" should {

    "change ANDs to ORs" in {
      oneLevelAndFilters.flatMap { case (f: And) =>
        val dm = deMorgan(f)
        dm.isInstanceOf[Or] must beTrue
        val dmChildren = dm.asInstanceOf[Or].getChildren

        f.getChildren.zip(dmChildren).map {
          case (origChild, dmChild) =>
            dmChild.isInstanceOf[Not] must beTrue
            dmChild.asInstanceOf[Not].getFilter mustEqual origChild
        }
      }
    }

    "change ORs to ANDs" in {
      oneLevelOrFilters.flatMap { case (f: Or) =>
        val dm = deMorgan(f)
        dm.isInstanceOf[And] must beTrue
        val dmChildren = dm.asInstanceOf[And].getChildren

        f.getChildren.zip(dmChildren).map {
          case (origChild, dmChild) =>
            dmChild.isInstanceOf[Not] must beTrue
            dmChild.asInstanceOf[Not].getFilter mustEqual origChild
        }
      }
    }

    "remove stacked NOTs" in {
      simpleNotFilters.map { case (f: Not) =>
        deMorgan(f) mustEqual f.getFilter
      }
    }
  }

  // Test logicDistributionDNF
  "The function 'logicDistributionDNF'" should {

    "split a top-level OR into a List of single-element Lists each containing a filter" in {
      (oneLevelOrFilters++oneLevelMultipleOrsFilters).flatMap { or =>
        val ll = logicDistributionDNF(or)
        ll.map { l => l.size mustEqual 1}
      }
    }

    "split a top-level AND into a singleton List which contains a List of the ANDed filters" in {

      oneLevelAndFilters.map { case (and: And) =>
        val ll = logicDistributionDNF(and)
        ll.size mustEqual 1

        and.getChildren.size mustEqual ll(0).size
      }
    }

    "not return filters with ANDs or ORs explicitly stated" in {
      // NB: The nested lists imply ANDs and ORs.
      andsOrsFilters.flatMap { filter: Filter =>
        val ll = logicDistributionDNF(filter)
        ll.flatten.map { l => l.isInstanceOf[BinaryLogicOperator] must beFalse}
      }
    }

    "take a 'simple' filter and return List(List(filter))" in {
      baseFilters.map { f =>
        val ll = logicDistributionDNF(f)
        ll.size mustEqual 1
        ll(0).size mustEqual 1
      }
    }
  }

  // Test logicDistributionCNF
  "The function 'logicDistributionCNF'" should {
    "split a top-level OR into a singleton List which contains a List of the ORed filters" in {
      oneLevelOrFilters.map { case (or: Or) =>
        val ll = logicDistributionCNF(or)
        ll.size mustEqual 1

        or.getChildren.size mustEqual ll(0).size
      }
    }

    "split a top-level AND into a a singleton List which contains a List of the ANDed filters" in {
      (oneLevelAndFilters++oneLevelMultipleAndsFilters).flatMap { and =>
        val ll = logicDistributionCNF(and)
        ll.map { l => l.size mustEqual 1}
      }
    }

    "not return filters with ANDs or ORs explicitly stated" in {
      // NB: The nested lists imply ANDs and ORs.
      andsOrsFilters.flatMap { filter: Filter =>
        val ll = logicDistributionCNF(filter)
        ll.flatten.map { l => l.isInstanceOf[BinaryLogicOperator] must beFalse}
      }
    }

    "take a 'simple' filter and return List(List(filter))" in {
      baseFilters.map { f =>
        val ll = logicDistributionCNF(f)
        ll.size mustEqual 1
        ll(0).size mustEqual 1
      }
    }
  }

  val mediumDataFeatures = mediumData.map(createSF)

  // Function defining rewriteFilter Properties.
  def testRewriteProps(filter: Filter): Fragments = {
    logger.debug(s"Filter: ${ECQL.toCQL(filter)}")

    "The function rewriteFilter" should {
      val rewrittenFilter: Filter = rewriteFilterInDNF(filter)

      "return a Filter with at most one OR at the top" in {
        val decomp = decomposeBinary(rewrittenFilter)
        val orCount = decomp.count(_.isInstanceOf[Or])

        orCount mustEqual 0
      }

      val children = decomposeOr(rewrittenFilter)

      "return a Filter where the children of the (optional) OR can (optionally) be an AND" in {
        children.map { _.isInstanceOf[Or] must beFalse }
      }

      "return a Filter where NOTs do not have ANDs or ORs as children" in {
        foreachWhen(children) { case f if f.isInstanceOf[Not] => f.isInstanceOf[BinaryLogicOperator] must beFalse }
      }

      "return a Filter which is 'equivalent' to the original filter" in {
        val originalCount = mediumDataFeatures.count(filter.evaluate)
        val rewriteCount = mediumDataFeatures.count(rewrittenFilter.evaluate)
        logger.debug(s"\nFilter: ${ECQL.toCQL(filter)}\nFullData size: ${mediumDataFeatures.size}: filter hits: $originalCount rewrite hits: $rewriteCount")
        rewriteCount mustEqual originalCount
      }
    }
  }

  oneGeomFilters.map(testRewriteProps)
}