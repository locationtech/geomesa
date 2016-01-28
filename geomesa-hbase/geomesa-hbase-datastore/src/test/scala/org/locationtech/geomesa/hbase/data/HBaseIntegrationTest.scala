/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.hbase.data

import org.geotools.data.store.ContentFeatureStore
import org.geotools.data.{DataUtilities, DataStoreFinder, Query, Transaction}
import org.geotools.factory.Hints
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

@RunWith(classOf[JUnitRunner])
class HBaseIntegrationTest extends Specification {

  "HBaseDataStore" should {
    "add features" in {

      skipped("integration")

      val typeName = "testsft"

      val ds = DataStoreFinder.getDataStore(Map(HBaseDataStore.BIGTABLENAMEPARAM.getName -> "test_sft"))
      if (!ds.getTypeNames.contains(typeName)) {
        ds.createSchema(SimpleFeatureTypes.createType(typeName, "name:String,dtg:Date,geom:Point:srid=4326"))
      }
      val sft = ds.getSchema(typeName)

      sft must not(beNull)

      val fs = ds.getFeatureSource(typeName).asInstanceOf[ContentFeatureStore]

      val toAdd = (0 until 10).map { i =>
        val sf = new ScalaSimpleFeature(i.toString, sft)
        sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        sf.setAttribute(0, s"name $i")
        sf.setAttribute(1, s"2014-01-01T0$i:00:00.000Z")
        sf.setAttribute(2, s"POINT(4$i 5$i)")
        sf
      }

      val ids = fs.addFeatures(toAdd)
      ids.asScala.map(_.getID) must containTheSameElementsAs((0 until 10).map(_.toString))

      val fr = ds.getFeatureReader(new Query(typeName, Filter.INCLUDE), Transaction.AUTO_COMMIT)
      fr.getFeatureType mustEqual sft
      val features = ArrayBuffer.empty[SimpleFeature]
      while (fr.hasNext) {
        features.append(fr.next())
      }
      features must haveLength(10)
      features must containTheSameElementsAs(toAdd)
    }

    "query features" in {

      skipped("integration")

      val typeName = "testsft"

      val ds = DataStoreFinder.getDataStore(Map(HBaseDataStore.BIGTABLENAMEPARAM.getName -> "test_sft"))

      val cql = "bbox(geom,-180,-90,180,90) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-01T23:59:59.000Z"
      val fr = ds.getFeatureReader(new Query(typeName, ECQL.toFilter(cql)), Transaction.AUTO_COMMIT)
      val features = ArrayBuffer.empty[SimpleFeature]
      while (fr.hasNext) {
        features.append(fr.next())
      }
      features must haveLength(10)
      features.map(DataUtilities.encodeFeature).foreach(println)
      success
    }
  }
}
