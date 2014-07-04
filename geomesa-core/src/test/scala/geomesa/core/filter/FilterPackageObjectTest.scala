package geomesa.core.filter

import com.typesafe.scalalogging.slf4j.Logging
import geomesa.core.filter.FilterUtils._
import geomesa.core.filter.TestFilters._
import geomesa.core.iterators.TestData._
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
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
        dm.isInstanceOf[Or] mustEqual true
        val dmChildren = dm.asInstanceOf[Or].getChildren

        f.getChildren.zip(dmChildren).map {
          case (origChild, dmChild) =>
            dmChild.isInstanceOf[Not] mustEqual true
            dmChild.asInstanceOf[Not].getFilter mustEqual origChild
        }
      }
    }

    "change ORs to ANDs" in {
      oneLevelOrFilters.flatMap { case (f: Or) =>
        val dm = deMorgan(f)
        dm.isInstanceOf[And] mustEqual true
        val dmChildren = dm.asInstanceOf[And].getChildren

        f.getChildren.zip(dmChildren).map {
          case (origChild, dmChild) =>
            dmChild.isInstanceOf[Not] mustEqual true
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

  // Test logicDistribution
  "The function 'logicDistribution'" should {

    "split a top-level OR into a List of single-element Lists each containing a filter" in {
      oneLevelOrFilters.flatMap { or =>
        val ll = logicDistribution(or)
        ll.map { l => l.size mustEqual 1}
      }
    }

    "split a top-level AND into a a singleton List which contains a List of the ANDed filters" in {

      oneLevelAndFilters.map { case (and: And) =>
        val ll = logicDistribution(and)
        ll.size mustEqual 1

        and.getChildren.size mustEqual ll(0).size
      }
    }

    "not return filters with ANDs or ORs explicitly stated" in {
      // NB: The nested lists imply ANDs and ORs.
      andsOrsFilters.flatMap { filter: Filter =>
        val ll = logicDistribution(filter)
        ll.flatten.map { l => l.isInstanceOf[BinaryLogicOperator] mustEqual false}
      }
    }

    "take a 'simple' filter and return List(List(filter))" in {
      baseFilters.map { f =>
        val ll = logicDistribution(f)
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
      val rewrittenFilter: Filter = rewriteFilter(filter)

      "return a Filter with at most one OR at the top" in {
        val decomp = decomposeBinary(rewrittenFilter)
        val orCount = decomp.count(_.isInstanceOf[Or])

        orCount mustEqual 0
      }

      val children = decomposeOr(rewrittenFilter)

      "return a Filter where the children of the (optional) OR can (optionally) be an AND" in {
        children.map { _.isInstanceOf[Or] mustEqual false }
      }

      "return a Filter where NOTs do not have ANDs or ORs as children" in {
        children.filter(_.isInstanceOf[Not]).foreach { f =>
          f.isInstanceOf[BinaryLogicOperator] mustEqual false
        }
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