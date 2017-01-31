/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.data

import org.geotools.data._
import org.geotools.factory.CommonFactoryFinder
import org.geotools.referencing.CRS
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AccumuloDataStoreIdlTest extends Specification with TestWithDataStore {

  sequential

  override val spec = "dtg:Date,*geom:Point:srid=4326"

  addFeatures((-180 to 180).map { lon =>
    val sf = new ScalaSimpleFeature(lon.toString, sft)
    sf.setAttribute(0, "2015-01-01T00:00:00.000Z")
    sf.setAttribute(1, s"POINT($lon ${lon / 10})")
    sf
  })

  val filterFactory = CommonFactoryFinder.getFilterFactory2
  val srs = CRS.toSRS(org.locationtech.geomesa.utils.geotools.CRS_EPSG_4326)

  "AccumuloDataStore" should {

    "handle IDL correctly" in {
      "default layer preview, bigger than earth, multiple IDL-wrapping geoserver BBOX" in {
        val filter = filterFactory.bbox("geom", -230, -110, 230, 110, srs)
        val query = new Query(sft.getTypeName, filter)
        val results = fs.getFeatures(query).features.map(_.getID)
        results must haveLength(361)
      }

      "greater than 180 lon diff non-IDL-wrapping geoserver BBOX" in {
        val filter = filterFactory.bbox("geom", -100, 1.1, 100, 4.1, srs)
        val query = new Query(sft.getTypeName, filter)
        val results = fs.getFeatures(query).features.map(_.getID)
        results must haveLength(30)
      }

      "small IDL-wrapping geoserver BBOXes" in {
        val spatial1 = filterFactory.bbox("geom", -181.1, -30, -175.1, 30, srs)
        val spatial2 = filterFactory.bbox("geom", 175.1, -30, 181.1, 30, srs)
        val filter = filterFactory.or(spatial1, spatial2)
        val query = new Query(sft.getTypeName, filter)
        val results = fs.getFeatures(query).features.map(_.getID)
        results must haveLength(10)
      }

      "large IDL-wrapping geoserver BBOXes" in {
        val spatial1 = filterFactory.bbox("geom", -181.1, -30, 40.1, 30, srs)
        val spatial2 = filterFactory.bbox("geom", 175.1, -30, 181.1, 30, srs)
        val filter = filterFactory.or(spatial1, spatial2)
        val query = new Query(sft.getTypeName, filter)
        val results = fs.getFeatures(query).features.map(_.getID)
        results must haveLength(226)
      }
    }
  }
}
