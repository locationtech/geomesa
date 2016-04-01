/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.filter

import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.filter.visitor.QueryPlanFilterVisitor
import org.opengis.filter._
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FilterHelperTest extends Specification {

  val ff = CommonFactoryFinder.getFilterFactory2

  def updateFilter(filter: Filter) = filter.accept(new QueryPlanFilterVisitor(null), null).asInstanceOf[Filter]

  "FilterHelper" should {

    "fix out of bounds bbox" in {
      val filter = ff.bbox(ff.property("geom"), -181, -91, 181, 91, "4326")
      val updated = updateFilter(filter)
      updated mustEqual Filter.INCLUDE
    }

    "be idempotent with bbox" in {
      val filter = ff.bbox(ff.property("geom"), -181, -91, 181, 91, "4326")
      val updated = updateFilter(filter)
      val reupdated = updateFilter(updated)
      ECQL.toCQL(updated) mustEqual ECQL.toCQL(reupdated)
    }

    "be idempotent with dwithin" in {
      val filter = ff.dwithin(ff.property("geom"), ff.literal("LINESTRING (-45 0, -90 45)"), 1000, "meters")
      val updated = updateFilter(filter)
      val reupdated = updateFilter(updated)
      ECQL.toCQL(updated) mustEqual ECQL.toCQL(reupdated)
    }

    "not modify valid intersects" in {
      val filter = ff.intersects(ff.property("geom"), ff.literal("POLYGON((45 23, 48 23, 48 27, 45 27, 45 23))"))
      val updated = updateFilter(filter)
      ECQL.toCQL(updated) mustEqual "INTERSECTS(geom, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23)))"
    }

    "fix IDL polygons in intersects" in {
      val filter = ff.intersects(ff.property("geom"), ff.literal("POLYGON((-150 23,-164 11,45 23,49 30,-150 23))"))
      val updated = updateFilter(filter)
      ECQL.toCQL(updated) mustEqual "INTERSECTS(geom, POLYGON ((-180 12.271523178807946, -180 24.304347826086957, " +
          "-150 23, -164 11, -180 12.271523178807946))) OR INTERSECTS(geom, POLYGON ((180 24.304347826086957, " +
          "180 12.271523178807946, 45 23, 49 30, 180 24.304347826086957)))"
    }

    "be idempotent with intersects" in {
      val filter = ff.intersects(ff.property("geom"), ff.literal("POLYGON((-150 23,-164 11,45 23,49 30,-150 23))"))
      val updated = updateFilter(filter)
      val reupdated = updateFilter(updated)
      ECQL.toCQL(updated) mustEqual ECQL.toCQL(reupdated)
    }
  }
}
