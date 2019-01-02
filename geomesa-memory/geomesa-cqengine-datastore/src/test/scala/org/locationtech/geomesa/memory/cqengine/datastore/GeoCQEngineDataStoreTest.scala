/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.memory.cqengine.datastore

import org.geotools.data.{DataStoreFinder, DataUtilities, Query}
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class GeoCQEngineDataStoreTest extends Specification {

  sequential

  private val feats = (0 until 1000).map(SampleFeatures.buildFeature)

  "GeoCQEngineData" should {

    val params = Map("cqengine" -> "true")
    val ds = DataStoreFinder.getDataStore(params)

    "get a datastore" in {
      ds mustNotEqual null
    }

    "createSchema" in {
      ds.createSchema(SampleFeatures.sft)
      ds.getTypeNames.length mustEqual 1
      ds.getTypeNames.contains("test") mustEqual true
    }

    "insert features" in {
      val fs = ds.getFeatureSource("test").asInstanceOf[GeoCQEngineFeatureStore]

      val fw = fs.getWriter(Query.ALL)
      fs.addFeatures(DataUtilities.collection(feats))

      fs mustNotEqual null
      fs.getCount(Query.ALL) mustEqual 1000
    }
  }
}
