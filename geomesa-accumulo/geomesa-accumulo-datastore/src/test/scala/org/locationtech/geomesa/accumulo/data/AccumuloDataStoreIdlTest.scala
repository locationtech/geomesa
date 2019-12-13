/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import org.geotools.data._
import org.geotools.referencing.CRS
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AccumuloDataStoreIdlTest extends Specification with TestWithDataStore {

  import org.locationtech.geomesa.filter.ff

  sequential

  override val spec = "dtg:Date,*geom:Point:srid=4326"

  addFeatures((-180 to 180).map { lon =>
    val sf = new ScalaSimpleFeature(sft, lon.toString)
    sf.setAttribute(0, "2015-01-01T00:00:00.000Z")
    sf.setAttribute(1, s"POINT($lon ${lon / 10})")
    sf
  })

  val srs = CRS.toSRS(org.locationtech.geomesa.utils.geotools.CRS_EPSG_4326)

  "AccumuloDataStore" should {

    "handle IDL correctly" in {
      "default layer preview, bigger than earth, multiple IDL-wrapping geoserver BBOX" in {
        val filter = ff.bbox("geom", -230, -110, 230, 110, srs)
        val query = new Query(sft.getTypeName, filter)
        val results = SelfClosingIterator(fs.getFeatures(query).features).toSeq
        results must haveLength(361)
      }

      "greater than 180 lon diff non-IDL-wrapping geoserver BBOX" in {
        val filter = ff.bbox("geom", -100, 1.1, 100, 4.1, srs)
        val query = new Query(sft.getTypeName, filter)
        val results = SelfClosingIterator(fs.getFeatures(query).features).toSeq
        results must haveLength(30)
      }

      "small IDL-wrapping geoserver BBOXes" in {
        val spatial1 = ff.bbox("geom", -181.1, -30, -175.1, 30, srs)
        val spatial2 = ff.bbox("geom", 175.1, -30, 181.1, 30, srs)
        val filter = ff.or(spatial1, spatial2)
        val query = new Query(sft.getTypeName, filter)
        val results = SelfClosingIterator(fs.getFeatures(query).features).toSeq
        results must haveLength(10)
      }

      "large IDL-wrapping geoserver BBOXes" in {
        val spatial1 = ff.bbox("geom", -181.1, -30, 40.1, 30, srs)
        val spatial2 = ff.bbox("geom", 175.1, -30, 181.1, 30, srs)
        val filter = ff.or(spatial1, spatial2)
        val query = new Query(sft.getTypeName, filter)
        val results = SelfClosingIterator(fs.getFeatures(query).features).toSeq
        results must haveLength(226)
      }
    }
  }
}
