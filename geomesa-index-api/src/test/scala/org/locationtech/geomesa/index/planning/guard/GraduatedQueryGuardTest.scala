/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.planning.guard

import java.io.StringReader

import com.typesafe.config.ConfigFactory
import org.geotools.data.{Query, Transaction}
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.index.TestGeoMesaDataStore
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GraduatedQueryGuardTest extends Specification {

  "GraduatedQueryGuard" should {
    "block queries with an excessive duration and spatial extent using graduated query guard" in {
      // note: z3 needs to be declared first so it's picked for full table scans
      val sft = SimpleFeatureTypes.createType("cea650aea6284b5281ee84c784cb56a7",
        "name:String,age:Int,dtg:Date,*geom:Point:srid=4326;geomesa.indices.enabled='z3,id,attr:name'")
      // NB: Uses configuration in the test reference.conf
      sft.getUserData.put("geomesa.query.interceptors",
        "org.locationtech.geomesa.index.planning.guard.GraduatedQueryGuard")

      val ds = new TestGeoMesaDataStore(true)
      ds.createSchema(sft)

      val valid = Seq(
        "name = 'bob'",
        "IN('123')",
        "bbox(geom,0,0,.2,.4) AND dtg during 2020-01-01T00:00:00.000Z/2020-02-01T00:00:00.000Z",
        // Three Corner cases.
        // Note that these periods are under the limit by two seconds.
        "bbox(geom,0,0,1,1) AND dtg during 2020-01-01T00:00:00.000Z/P60D",
        "bbox(geom,0,0,2,5) AND dtg during 2020-01-01T00:00:00.000Z/P3D",
        "bbox(geom,-180,-90,180,90) AND dtg during 2020-01-01T00:00:00.000Z/P1D",
        "bbox(geom,0,0,2,4) AND dtg during 2020-01-01T00:00:00.000Z/2020-01-02T00:00:00.000Z",
        "bbox(geom,-10,-10,10,10) AND dtg during 2020-01-01T00:00:00.000Z/2020-01-01T23:00:00.000Z",
        "bbox(geom,-10,-10,10,10) AND (dtg during 2020-01-01T00:00:00.000Z/2020-01-01T00:59:59.000Z OR dtg during 2020-01-01T12:00:00.000Z/2020-01-01T12:59:59.000Z)"
      )

      val invalid = Seq(
        "INCLUDE",
        "bbox(geom,-10,-10,10,10)",
        "bbox(geom,-180,-90,180,90)",
        // Corner cases.  During seems to exclude the start and end.
        // To get a period of a given length one needs to add 2 seconds.
        "bbox(geom,0,0,1,1) AND dtg during 2020-01-01T00:00:00.000Z/P60DT3S",
        "bbox(geom,0,0,2,5) AND dtg during 2020-01-01T00:00:00.000Z/P3DT3S",
        "bbox(geom,-180,-90,180,90) AND dtg during 2020-01-01T00:00:00.000Z/P1DT3S",
        "dtg during 2020-01-01T00:00:00.000Z/P1DT3S",
        "bbox(geom,0,0,.2,.4) AND dtg during 2020-01-01T00:00:00.000Z/2020-04-02T00:00:00.000Z",
        "bbox(geom,0,0,2,4) AND dtg during 2020-01-01T00:00:00.000Z/2020-01-05T00:00:00.000Z",
        "bbox(geom,-10,-10,10,10) AND dtg during 2020-01-01T00:00:00.000Z/2020-01-03T00:00:00.000Z",
        "bbox(geom,-10,-10,10,10) AND dtg after 2020-01-01T00:00:00.000Z",
        "dtg after 2020-01-01T00:00:00.000Z"
      )

      foreach(valid.map(ECQL.toFilter)) { filter =>
        SelfClosingIterator(ds.getFeatureReader(new Query(sft.getTypeName, filter), Transaction.AUTO_COMMIT)).toList must
          beEmpty
      }

      foreach(invalid.map(ECQL.toFilter)) { filter =>
        SelfClosingIterator(ds.getFeatureReader(new Query(sft.getTypeName, filter), Transaction.AUTO_COMMIT)).toList must
          throwAn[IllegalArgumentException]
      }

      // create a new store so the sys prop gets evaluated when the query guards are loaded
      val ds2 = new TestGeoMesaDataStore(true)
      System.setProperty(s"geomesa.guard.graduated.${sft.getTypeName}.disable", "true")
      try {
        ds2.createSchema(sft)
        foreach(invalid.map(ECQL.toFilter)) { filter =>
          SelfClosingIterator(ds2.getFeatureReader(new Query(sft.getTypeName, filter), Transaction.AUTO_COMMIT)).toList must
              beEmpty
        }
      } finally {
        System.clearProperty(s"geomesa.guard.graduated.${sft.getTypeName}.disable")
        ds2.dispose()
      }
    }
    "graduated guard needs to be valid" in {
      val configString =
        """
          | "out-of-order" = [
          |   { size = 1,  duration = "3 days"  }
          |   { size = 10, duration = "60 days" }
          |   { duration = "1 day" }
          | ]
          |  "repeated-size" = [
          |   { size = 1,  duration = "3 days"  }
          |   { size = 1, duration = "60 days" }
          |   { duration = "1 day" }
          | ]
          |   "no-upper-bound" = [
          |   { size = 1,  duration = "3 days"  }
          |   { size = 1, duration = "60 days" }
          | ]
          |""".stripMargin

      forall(Seq("out-of-order", "repeated-size", "no-upper-bound")) {
        path =>
          val configList = ConfigFactory.parseReader(new StringReader(configString)).getConfigList(path)
          GraduatedQueryGuard.buildLimits(configList) must throwAn[IllegalArgumentException]
      }
    }
  }
}


