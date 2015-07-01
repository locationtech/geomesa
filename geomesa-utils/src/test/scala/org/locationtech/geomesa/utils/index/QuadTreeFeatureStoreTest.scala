/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.utils.index

import com.vividsolutions.jts.geom.{Coordinate, Geometry}
import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.geometry.jts.JTSFactoryFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class QuadTreeFeatureStoreTest extends Specification {

  "QuadTreeFeatureStore" should {
    val testsft = SimpleFeatureTypes.createType("test", "name:String,geom:Point:srid=4326")
    val gf = JTSFactoryFinder.getGeometryFactory()
    val builder = new SimpleFeatureBuilder(testsft)
    val qtfs = new QuadTreeFeatureStore {
      override def sft: SimpleFeatureType = testsft
      override val spatialIndex = new SynchronizedQuadtree[SimpleFeature]
    }

    builder.addAll(Array[AnyRef]("one", gf.createPoint(new Coordinate(48.9,80))))
    val f1 = builder.buildFeature("one")
    builder.reset()
    builder.addAll(Array[AnyRef]("two", gf.createPoint(new Coordinate(49.5,80))))
    val f2 = builder.buildFeature("two")

    qtfs.spatialIndex.insert(f1.getDefaultGeometry.asInstanceOf[Geometry].getEnvelopeInternal, f1)
    qtfs.spatialIndex.insert(f2.getDefaultGeometry.asInstanceOf[Geometry].getEnvelopeInternal, f2)

    "properly handle bbox queries" in {
      import org.locationtech.geomesa.utils.geotools.Conversions._

      val ff = CommonFactoryFinder.getFilterFactory2
      val bboxFilter = ff.bbox("geom", 49.0, 79.0, 51.0, 81.0, "EPSG:4326")

      qtfs.bbox(bboxFilter).getIterator.toList.size must be equalTo 1
    }
  }
}
