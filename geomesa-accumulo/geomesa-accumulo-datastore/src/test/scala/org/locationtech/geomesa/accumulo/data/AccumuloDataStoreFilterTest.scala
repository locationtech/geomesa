/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import org.geotools.data._
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AccumuloDataStoreFilterTest extends Specification with TestWithDataStore {

  sequential

  // note: index=full on the geometry tests a regression bug in stats (GEOMESA-1292)
  override val spec = "name:String,dtg:Date,*geom:Geometry:srid=4326:index=full;geomesa.mixed.geometries='true'"

  val point = {
    val sf = new ScalaSimpleFeature(sft, "point")
    sf.setAttribute(0, "name-point")
    sf.setAttribute(1, "2014-01-01T00:00:00.000Z")
    sf.setAttribute(2, "POINT (-120 45)")
    sf
  }
  val polygon = {
    val sf = new ScalaSimpleFeature(sft, "poly")
    sf.setAttribute(0, "name-poly")
    sf.setAttribute(1, "2014-01-01T00:00:00.000Z")
    sf.setAttribute(2, "POLYGON((-120 45, -120 50, -125 50, -125 45, -120 45))")
    sf
  }
  addFeatures(Seq(point, polygon))

  "AccumuloDataStore" should {
    "query by point type" in {
      val filter = ECQL.toFilter("geometryType(geom) = 'Point'")
      val query = new Query(sftName, filter)
      val features = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
      features must haveLength(1)
      features.head mustEqual point
    }
    "query by polygon type" in {
      val filter = ECQL.toFilter("geometryType(geom) = 'Polygon'")
      val query = new Query(sftName, filter)
      val features = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
      features must haveLength(1)
      features.head mustEqual polygon
    }
  }
}
