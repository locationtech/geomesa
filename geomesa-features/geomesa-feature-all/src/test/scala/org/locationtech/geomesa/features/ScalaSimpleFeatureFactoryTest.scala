/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features

import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.geometry.GeometryBuilder
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ScalaSimpleFeatureFactoryTest extends Specification {

  "GeoTools must use KryoSimpleFeatureFactory when hint is set" in {
    ScalaSimpleFeatureFactory.init

    val featureFactory = CommonFactoryFinder.getFeatureFactory(null)
    featureFactory.getClass mustEqual classOf[ScalaSimpleFeatureFactory]
  }

  "SimpleFeatureBuilder should return an KryoSimpleFeature when using an KryoSimpleFeatureFactory" in {
    ScalaSimpleFeatureFactory.init
    val geomBuilder = new GeometryBuilder(DefaultGeographicCRS.WGS84)
    val featureFactory = CommonFactoryFinder.getFeatureFactory(null)
    val sft = SimpleFeatureTypes.createType("testkryo", "name:String,geom:Point:srid=4326")
    val builder = new SimpleFeatureBuilder(sft, featureFactory)
    builder.reset()
    builder.add("Hello")
    builder.add(geomBuilder.createPoint(1,1))
    val feature = builder.buildFeature("id")

    feature must beAnInstanceOf[ScalaSimpleFeature]
  }

}
