/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import org.geotools.data._
import org.geotools.filter.text.cql2.CQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class LiveAccumuloDataStoreTest extends Specification {

  sequential

  val sftName = "mysft"

  val params = Map(
    "instanceId"        -> "mycloud",
    "zookeepers"        -> "zoo1,zoo2,zoo3",
    "user"              -> "user",
    "password"          -> "password",
    "tableName"         -> "geomesa.data")

  "AccumuloDataStore" should {

    "run live tests" in {

      skipped("Meant for integration testing")

      val ds = DataStoreFinder.getDataStore(params).asInstanceOf[AccumuloDataStore]

      val query = new Query(sftName, CQL.toFilter("INCLUDE"))

      // get the feature store used to query the GeoMesa data
      val featureStore = ds.getFeatureSource(sftName)

      // execute the query
      val results = featureStore.getFeatures(query).features

      try {
        CloseableIterator(results).foreach(sf => println(DataUtilities.encodeFeature(sf)))
      } finally {
        results.close()
      }

      success
    }
  }

}
