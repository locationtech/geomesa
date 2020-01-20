/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.memory.cqengine.datastore

import org.geotools.data.{DataStoreFinder, DataUtilities, Query, Transaction}
import org.geotools.filter.text.ecql.ECQL
import org.geotools.util.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class GeoCQEngineDataStoreTest extends Specification {

  sequential

  private val feats = Seq.tabulate[SimpleFeature](1000) { i =>
    val f = SampleFeatures.buildFeature(i)
    f.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
    f
  }

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
      fs must not(beNull)
      fs.addFeatures(DataUtilities.collection(feats))
      fs.getCount(Query.ALL) mustEqual 1000
      SelfClosingIterator(fs.getFeatures().features()).map(_.getID).toList.sorted mustEqual feats.map(_.getID).sorted
    }

    "not allow feature modification" in {
      val query = new Query("test", ECQL.toFilter("IN ('1')"))
      val result = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
      result must haveLength(1)
      result.head.setAttribute(0, "update") must throwAn[UnsupportedOperationException]
    }
  }
}
