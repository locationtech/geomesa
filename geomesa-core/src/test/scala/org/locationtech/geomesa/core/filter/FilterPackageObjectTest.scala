package org.locationtech.geomesa.core.filter

import com.typesafe.scalalogging.slf4j.Logging
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.core.filter.TestFilters._
import org.locationtech.geomesa.core.iterators.TestData._
import org.opengis.filter._
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.Fragments

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class FilterPackageObjectTest extends Specification with Logging {

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