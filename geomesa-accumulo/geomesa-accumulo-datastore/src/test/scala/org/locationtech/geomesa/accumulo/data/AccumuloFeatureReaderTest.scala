/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.data

import org.geotools.data.Query
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo._
import org.locationtech.geomesa.accumulo.index.ExplainString
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AccumuloFeatureReaderTest extends Specification with TestWithDataStore {

  override def spec = s"foo:String,baz:Date,dtg:Date,*geom:Geometry"

  "AccumuloFeatureReader" should {

    "be able to run explainQuery" in {
      val query = new Query(sftName)
      val fs = "INTERSECTS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28)))"
      val f = ECQL.toFilter(fs)
      query.setFilter(f)

      val out = new ExplainString()
      ds.explainQuery(query, out)

      val explanation = out.toString()
      explanation must not be null
    }
  }
}
