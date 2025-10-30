/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.hbase.data

import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.data._
import org.geotools.api.feature.simple.SimpleFeature
import org.geotools.api.filter.Filter
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.util.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.hbase.data.HBaseDataStoreParams._
import org.locationtech.geomesa.process.query.ProximitySearchProcess
import org.locationtech.geomesa.process.tube.TubeSelectProcess
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class HBaseProcessTest extends Specification with LazyLogging {

  import scala.collection.JavaConverters._

  sequential

  "HBaseDataStore" should {
    "work with wps processes" in {
      val typeName = "testpoints"

      val params = Map(
        ConfigsParam.getName -> HBaseCluster.hbaseSiteXml,
        HBaseCatalogParam.getName -> getClass.getSimpleName)
      val ds = DataStoreFinder.getDataStore(params.asJava).asInstanceOf[HBaseDataStore]
      ds must not(beNull)

      try {
        ds.getSchema(typeName) must beNull

        ds.createSchema(SimpleFeatureTypes.createType(typeName, "name:String:index=true,attr:String,dtg:Date,*geom:Point:srid=4326"))

        val sft = ds.getSchema(typeName)

        sft must not(beNull)

        val fs = ds.getFeatureSource(typeName).asInstanceOf[SimpleFeatureStore]

        val toAdd = (0 until 10).map { i =>
          val sf = new ScalaSimpleFeature(sft, i.toString)
          sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
          sf.setAttribute(0, s"name$i")
          sf.setAttribute(1, s"name$i")
          sf.setAttribute(2, f"2014-01-${i + 1}%02dT00:00:01.000Z")
          sf.setAttribute(3, s"POINT(4$i 5$i)")
          sf: SimpleFeature
        }

        val ids = fs.addFeatures(new ListFeatureCollection(sft, toAdd.asJava))
        ids.asScala.map(_.getID) must containTheSameElementsAs((0 until 10).map(_.toString))

        val query = new ListFeatureCollection(sft, toAdd(4))
        val source = ds.getFeatureSource(typeName).getFeatures()

        val proximity = new ProximitySearchProcess().execute(query, source, 10.0)
        SelfClosingIterator(proximity.features()).toList mustEqual Seq(toAdd(4))

        val tube = new TubeSelectProcess().execute(query, source, Filter.INCLUDE, null, 1L, 100.0, 10, null)
        SelfClosingIterator(tube.features()).toList mustEqual Seq(toAdd(4))
      } finally {
        ds.dispose()
      }
    }
  }
}
