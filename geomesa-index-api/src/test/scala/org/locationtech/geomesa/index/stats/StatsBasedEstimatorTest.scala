/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.stats

import org.geotools.data.Query
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.factory.Hints
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.TestGeoMesaDataStore
import org.locationtech.geomesa.index.index.z3.Z3Index
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class StatsBasedEstimatorTest extends Specification {

  val sft = SimpleFeatureTypes.createType("test", "trackId:String:index=true,dtg:Date,*geom:Point:srid=4326")

  val ds = new TestGeoMesaDataStore(true)

  val features = Seq.tabulate(10) { i =>
    ScalaSimpleFeature.create(sft, s"$i", s"track-$i", f"2018-01-01T$i%02d:00:00.000Z", s"POINT (4$i 55)")
  }

  step {
    ds.createSchema(sft)
    features.foreach(_.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE))
    ds.getFeatureSource(sft.getTypeName).addFeatures(new ListFeatureCollection(sft, features.toArray[SimpleFeature]))
  }

  "StatsBasedEstimator" should {
    "handle not null counts" in {
      ds.stats.getCount(sft, ECQL.toFilter("trackId is not null")) must beSome(10L)
    }
    "select better query plans over not null" in {
      val filter = ECQL.toFilter("NOT (trackId IS NULL) AND " +
          "dtg > 2018-01-01T00:00:00+00:00 AND dtg < 2018-01-01T08:00:00+00:00 AND " +
          "CONTAINS(POLYGON ((44 54, 44 56, 48 56, 48 54, 44 54)), geom) AND " +
          "NOT (dtg IS NULL) AND " +
          "INCLUDE")
      val plans = ds.getQueryPlan(new Query(sft.getTypeName, filter, Array("trackId", "dtg")))
      plans must haveLength(1)
      plans.head.filter.index must beAnInstanceOf[Z3Index]
    }
  }

  step {
    ds.dispose()
  }
}
