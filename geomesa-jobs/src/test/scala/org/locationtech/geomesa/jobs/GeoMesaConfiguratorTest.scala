/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.jobs

import org.apache.hadoop.conf.Configuration
import org.geotools.referencing.CRS
import org.junit.runner.RunWith
import org.locationtech.geomesa.index.api.QueryPlan.{FeatureReducer, ResultsToFeatures}
import org.locationtech.geomesa.index.iterators.StatsScan.StatsReducer
import org.locationtech.geomesa.index.utils.Reprojection.QueryReferenceSystems
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GeoMesaConfiguratorTest extends Specification {

  val sft = SimpleFeatureTypes.createType("test", "name:String,dtg:Date,*geom:Point:srid=4326")

  "GeoMesaConfigurator" should {
    "set and retrieve data store params" in {
      val conf = new Configuration()
      val params = Map("user" -> "myuser2", "password" -> "mypassword2", "instance" -> "myinstance2")
      GeoMesaConfigurator.setDataStoreOutParams(conf, params)
      val recovered = GeoMesaConfigurator.getDataStoreOutParams(conf)
      recovered mustEqual params
    }

    "set and retrieve results to features" in {
      val conf = new Configuration()
      val toFeatures = ResultsToFeatures.identity(sft)
      GeoMesaConfigurator.setResultsToFeatures(conf, toFeatures)
      val recovered = GeoMesaConfigurator.getResultsToFeatures(conf)
      recovered mustEqual toFeatures
    }

    "set and retrieve feature reducers" in {
      val conf = new Configuration()
      val reducer = new StatsReducer(sft, "MinMax(dtg)", true)
      GeoMesaConfigurator.setReducer(conf, reducer)
      val recovered = GeoMesaConfigurator.getReducer(conf)
      recovered must beSome[FeatureReducer](reducer)
    }

    "set and retrieve sorting" in {
      val conf = new Configuration()
      val sorting = Seq(("dtg", true), ("name", false))
      GeoMesaConfigurator.setSorting(conf, sorting)
      val recovered = GeoMesaConfigurator.getSorting(conf)
      recovered must beSome(sorting)
    }

    "set and retrieve relational projections" in {
      val conf = new Configuration()
      val proj = QueryReferenceSystems(CRS.decode("EPSG:4326"), CRS.decode("EPSG:4326"), CRS.decode("EPSG:3587"))
      GeoMesaConfigurator.setProjection(conf, proj)
      val recovered = GeoMesaConfigurator.getProjection(conf)
      recovered must beSome(proj)
    }
  }
}
