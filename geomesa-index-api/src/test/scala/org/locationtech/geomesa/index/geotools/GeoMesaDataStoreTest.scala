/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.geotools

import org.locationtech.jts.geom.{Geometry, Point}
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.{Query, Transaction}
import org.geotools.factory.Hints
import org.geotools.geometry.jts.JTS
import org.geotools.referencing.CRS
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.TestGeoMesaDataStore
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GeoMesaDataStoreTest extends Specification {

  import org.locationtech.geomesa.utils.geotools.CRS_EPSG_4326

  val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")

  val ds = new TestGeoMesaDataStore(true)
  ds.createSchema(sft)

  val features = Seq.tabulate(10) { i =>
    ScalaSimpleFeature.create(sft, s"$i", s"name$i", i, f"2018-01-01T$i%02d:00:00.000Z", s"POINT (4$i 55)")
  }

  val epsg3857 = CRS.decode("EPSG:3857")

  step {
    features.foreach(_.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE))
    ds.getFeatureSource(sft.getTypeName).addFeatures(new ListFeatureCollection(sft, features.toArray[SimpleFeature]))
  }

  "GeoMesaDataStore" should {
    "reproject geometries" in {
      val query = new Query("test")
      query.setCoordinateSystemReproject(epsg3857)
      val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toSeq
      results must haveLength(10)

      val transform = CRS.findMathTransform(epsg3857, CRS_EPSG_4326, true)

      foreach(results) { result =>
        result.getFeatureType.getGeometryDescriptor.getCoordinateReferenceSystem mustEqual epsg3857
        val recovered = JTS.transform(result.getDefaultGeometry.asInstanceOf[Geometry], transform).asInstanceOf[Point]
        val expected = features.find(_.getID == result.getID).get.getDefaultGeometry.asInstanceOf[Point]
        recovered.getX must beCloseTo(expected.getX, 0.001)
        recovered.getY must beCloseTo(expected.getY, 0.001)
      }
    }
  }
}
