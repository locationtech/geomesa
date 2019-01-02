/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.filter

import com.typesafe.scalalogging.LazyLogging
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.junit.runner.RunWith
import org.locationtech.geomesa.filter.visitor.BoundsFilterVisitor
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner


@RunWith(classOf[JUnitRunner])
class BoundsFilterVisitorTest extends Specification with LazyLogging {

  "BoundsFilterVisitor" should {
    "work for during" >> {
      val filter = ECQL.toFilter("(BBOX(geom,0,0,1,1)) AND (dtg DURING 2016-07-01T20:00:00.000Z/2016-07-01T21:00:00.000Z)")
      BoundsFilterVisitor.visit(filter) mustEqual new ReferencedEnvelope(0.0, 1.0, 0.0, 1.0, DefaultGeographicCRS.WGS84)
    }
  }

}


