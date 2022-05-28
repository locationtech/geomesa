/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.planning.guard

import org.geotools.data.{Query, Transaction}
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.index.TestGeoMesaDataStore
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TemporalQueryGuardTest extends Specification {


  "TemporalQueryGuard" should {
    "block queries with an excessive duration" in {
      // note: z3 needs to be declared first so it's picked for full table scans
      val sft = SimpleFeatureTypes.createType("c4f4ef29-6e41-4113-9b74-adf35711aa7a",
        "name:String,age:Int,dtg:Date,*geom:Point:srid=4326;geomesa.indices.enabled='z3,id,attr:name'")
      sft.getUserData.put("geomesa.query.interceptors",
        "org.locationtech.geomesa.index.planning.guard.TemporalQueryGuard");
      sft.getUserData.put("geomesa.guard.temporal.max.duration", "1 day")

      val ds = new TestGeoMesaDataStore(true)
      ds.createSchema(sft)

      val valid = Seq(
        "name = 'bob'",
        "IN('123')",
        "bbox(geom,-10,-10,10,10) AND dtg during 2020-01-01T00:00:00.000Z/2020-01-01T23:59:59.000Z",
        "bbox(geom,-10,-10,10,10) AND (dtg during 2020-01-01T00:00:00.000Z/2020-01-01T00:59:59.000Z OR dtg during 2020-01-01T12:00:00.000Z/2020-01-01T12:59:59.000Z)"
      )

      val invalid = Seq(
        "INCLUDE",
        "bbox(geom,-10,-10,10,10)",
        "bbox(geom,-180,-90,180,90)",
        "bbox(geom,-10,-10,10,10) AND dtg during 2020-01-01T00:00:00.000Z/2020-01-03T23:59:59.000Z",
        "bbox(geom,-10,-10,10,10) AND dtg after 2020-01-01T00:00:00.000Z"
      )

      foreach(valid.map(ECQL.toFilter)) { filter =>
        SelfClosingIterator(ds.getFeatureReader(new Query(sft.getTypeName, filter), Transaction.AUTO_COMMIT)).toList must
            beEmpty
      }

      foreach(invalid.map(ECQL.toFilter)) { filter =>
        SelfClosingIterator(ds.getFeatureReader(new Query(sft.getTypeName, filter), Transaction.AUTO_COMMIT)).toList must
            throwAn[IllegalArgumentException]
      }
      ds.dispose()

      // create a new store so the sys prop gets evaluated when the query guards are loaded
      val ds2 = new TestGeoMesaDataStore(true)
      System.setProperty(s"geomesa.guard.temporal.${sft.getTypeName}.disable", "true")
      try {
        ds2.createSchema(sft)
        foreach(invalid.map(ECQL.toFilter)) { filter =>
          SelfClosingIterator(ds2.getFeatureReader(new Query(sft.getTypeName, filter), Transaction.AUTO_COMMIT)).toList must
              beEmpty
        }
      } finally {
        System.clearProperty(s"geomesa.guard.temporal.${sft.getTypeName}.disable")
        ds2.dispose()
      }
    }
  }
}


