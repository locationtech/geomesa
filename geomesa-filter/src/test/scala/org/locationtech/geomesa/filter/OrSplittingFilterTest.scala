/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.filter

import org.geotools.filter.text.cql2.CQL
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.opengis.filter._
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class OrSplittingFilterTest extends Specification {

  val geom1: Filter = CQL.toFilter("INTERSECTS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28)))")
  val geom2: Filter = CQL.toFilter("INTERSECTS(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23)))")
  val date1: Filter = CQL.toFilter("(dtg between '0000-01-01T00:00:00.000Z' AND '9999-12-31T23:59:59.000Z')")

  val osf = new OrSplittingFilter

  def splitFilter(f: Filter) = osf.visit(f, null)

  implicit class RichFilter(val filter: Filter) {
    def &&(that: Filter) = ff.and(filter, that)

    def ||(that: Filter) = ff.or(filter, that)

    def ! = ff.not(filter)
  }

  implicit def stringToFilter(s: String): Filter = ECQL.toFilter(s)

  implicit def intToAttributeFilter(i: Int): Filter = s"attr$i = val$i"

  implicit def intToFilter(i: Int): RichFilter = intToAttributeFilter(i)

  "The OrSplittingFilter" should {

    "not do anything to filters without a top-level OR" in {
      val filterStrings = Seq(geom1, geom1 && date1, 1 && 2, (3 && 4).!, (1 || 3).!)

      forall(filterStrings) { f => Seq(f) mustEqual splitFilter(f) }
    }

    "split an OR into two pieces" in {
      val orStrings = Seq(geom1 || geom2, geom2 || date1, 1 || 2, geom1 || 3)

      forall(orStrings) { f =>
        splitFilter(f).size mustEqual 2
      }
    }

    "recursively split nested ORs" in {
      val nested = Seq((geom1 || date1) || geom2, 1 || 2 || 3, 1 || (2 && 3) || 4, 1 || (geom2 || date1))
      forall(nested) { f =>
        splitFilter(f).size mustEqual 3
      }
    }

    "not run through lower-level filters" in {
      val filter = (3 || 4).! || (1 && 2)
      splitFilter(filter).size mustEqual 2
    }
  }
}
