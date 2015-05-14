/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.features.avro

import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.geometry.GeometryBuilder
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AvroSimpleFeatureFactoryTest extends Specification {

  "GeoTools must use AvroSimpleFeatureFactory when hint is set" in {
    AvroSimpleFeatureFactory.init

    val featureFactory = CommonFactoryFinder.getFeatureFactory(null)
    featureFactory.getClass mustEqual classOf[AvroSimpleFeatureFactory]
  }


  "SimpleFeatureBuilder should return an AvroSimpleFeature when using an AvroSimpleFeatureFactory" in {
    AvroSimpleFeatureFactory.init
    val geomBuilder = new GeometryBuilder(DefaultGeographicCRS.WGS84)
    val featureFactory = CommonFactoryFinder.getFeatureFactory(null)
    val sft = SimpleFeatureTypes.createType("testavro", "name:String,geom:Point:srid=4326")
    val builder = new SimpleFeatureBuilder(sft, featureFactory)
    builder.reset()
    builder.add("Hello")
    builder.add(geomBuilder.createPoint(1,1))
    val feature = builder.buildFeature("id")

    feature.getClass mustEqual classOf[AvroSimpleFeature]
  }

}
