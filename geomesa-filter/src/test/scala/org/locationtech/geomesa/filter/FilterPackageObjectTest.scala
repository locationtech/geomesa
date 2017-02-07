/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.filter

import com.typesafe.scalalogging.LazyLogging
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.filter._
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class FilterPackageObjectTest extends org.specs2.mutable.Spec
    with org.specs2.matcher.SequenceMatchersCreation with LazyLogging {

  import TestFilters._

  "The partitionGeom function" should {
    val sft = SimpleFeatureTypes.createType("filterPackageTest", "g:Geometry,*geom:Geometry")
    val geomFilter = ECQL.toFilter("BBOX(geom, -45.0,-45.0,45.0,45.0)")

    "filter bbox based on default geom" in {
      val filter = ECQL.toFilter("BBOX(geom, -45.0,-45.0,45.0,45.0) AND BBOX(g, -30.0,-30.0,30.0,30.0)")
      val (geoms, nongeoms) = partitionPrimarySpatials(filter, sft)
      geoms must haveLength(1)
      nongeoms must haveLength(1)
      ECQL.toCQL(geoms(0)) mustEqual("BBOX(geom, -45.0,-45.0,45.0,45.0)")
      ECQL.toCQL(nongeoms(0)) mustEqual("BBOX(g, -30.0,-30.0,30.0,30.0)")
    }
    "filter intersect based on default geom" in {
      val filter =
        ECQL.toFilter("INTERSECTS(geom, POLYGON ((-45 -45, -45 45, 45 45, 45 -45, -45 -45))) " +
          "AND INTERSECTS(g, POLYGON ((-30 -30, -30 30, 30 30, 30 -30, -30 -30)))")
      val (geoms, nongeoms) = partitionPrimarySpatials(filter, sft)
      geoms must haveLength(1)
      nongeoms must haveLength(1)
      ECQL.toCQL(geoms(0)) mustEqual("INTERSECTS(geom, POLYGON ((-45 -45, -45 45, 45 45, 45 -45, -45 -45)))")
      ECQL.toCQL(nongeoms(0)) mustEqual("INTERSECTS(g, POLYGON ((-30 -30, -30 30, 30 30, 30 -30, -30 -30)))")
    }
    "filter intersect based on default geom regardless of order" in {
      val filter =
        ECQL.toFilter("INTERSECTS(POLYGON ((-30 -30, -30 30, 30 30, 30 -30, -30 -30)), g) " +
            "AND INTERSECTS(POLYGON ((-45 -45, -45 45, 45 45, 45 -45, -45 -45)), geom)")
      val (geoms, nongeoms) = partitionPrimarySpatials(filter, sft)
      geoms must haveLength(1)
      nongeoms must haveLength(1)
      ECQL.toCQL(geoms(0)) mustEqual("INTERSECTS(POLYGON ((-45 -45, -45 45, 45 45, 45 -45, -45 -45)), geom)")
      ECQL.toCQL(nongeoms(0)) mustEqual("INTERSECTS(POLYGON ((-30 -30, -30 30, 30 30, 30 -30, -30 -30)), g)")
    }

    "handle ANDs with multiple predicates" in {
      val filters= Seq(1 && geomFilter && 2, 1 && 2 && geomFilter, geomFilter && 1 && 2)

      forall(filters) { filter => 
        val (geoms, nongeoms) = partitionPrimarySpatials(filter, sft)
        geoms must haveLength(1)
        nongeoms must haveLength(2)
      }
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

  // Function defining rewriteFilter Properties.
  oneGeomFilters.map { filter =>
    val ecql = ECQL.toCQL(filter)
    logger.debug(s"Filter: $ecql")

    def breakUpOr(f: Filter): Seq[Filter] = {
      f match {
        case or: Or => or.getChildren
        case _ => Seq(f)
      }
    }

    s"The function rewriteFilter for $ecql" >> {
      val rewrittenFilter: Filter = rewriteFilterInDNF(filter)

      "return a Filter with at most one OR at the top" in {
        val decomp = breakUpOr(rewrittenFilter)
        val orCount = decomp.count(_.isInstanceOf[Or])

        orCount mustEqual 0
      }

      val children = decomposeOr(rewrittenFilter)

      s"return a Filter where the children of the (optional) OR can (optionally) be an AND for $ecql" in {
        children.map { _.isInstanceOf[Or] must beFalse }
      }

      s"return a Filter where NOTs do not have ANDs or ORs as children for $ecql" in {
        foreachWhen(children) { case f if f.isInstanceOf[Not] => f.isInstanceOf[BinaryLogicOperator] must beFalse }
      }
    }
  }
}
