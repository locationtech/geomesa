/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.filter.index

import org.locationtech.jts.geom.Geometry
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.index.{SpatialIndex, SynchronizedQuadtree}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SpatialIndexSupportTest extends Specification {

  val _sft = SimpleFeatureTypes.createType("test", "name:String,*geom:Point:srid=4326,geom2:Point:srid=4326")
  val f1 = ScalaSimpleFeature.create(_sft, "one", "one", "POINT(48.9 80)", "POINT(38.9 80)")
  val f2 = ScalaSimpleFeature.create(_sft, "two", "two", "POINT(49.5 80)", "POINT(39.5 80)")

  val sis = new SpatialIndexSupport() {
    override val sft: SimpleFeatureType = _sft
    override val index: SpatialIndex[SimpleFeature] = new SynchronizedQuadtree[SimpleFeature]
  }

  step {
    sis.index.insert(f1.getDefaultGeometry.asInstanceOf[Geometry].getEnvelopeInternal, f1.getID, f1)
    sis.index.insert(f2.getDefaultGeometry.asInstanceOf[Geometry].getEnvelopeInternal, f2.getID, f2)
  }

  "SpatialIndexSupport" should {
    "properly handle bbox queries" in {
      val filter = ECQL.toFilter("bbox(geom, 49.0, 79.0, 51.0, 81.0)")
      sis.query(filter).toList mustEqual List(f2)
    }

    "properly handle bbox queries on secondary geometries" in {
      val filter = ECQL.toFilter("bbox(geom2, 38.0, 79.0, 39.0, 81.0)")
      sis.query(filter).toList mustEqual List(f1)
    }
  }
}
