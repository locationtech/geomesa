/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.filter.function

import org.locationtech.jts.geom.Point
import org.geotools.feature.{NameImpl, AttributeTypeBuilder}
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.filter.spatial.BBOXImpl
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FastPropertyTest extends Specification {

  "FastProperty" should {
    "evaluate" >> {
      val filter = ECQL.toFilter("bbox(geom,-180,-90,180,90)")
      val sft = SimpleFeatureTypes.createType("FastPropertyTest", "name:String,*geom:Point:srid=4326")
      val sf = new ScalaSimpleFeature(sft, "id")
      sf.setAttributes(Array[AnyRef]("myname", "POINT(45 45)"))
      filter.asInstanceOf[BBOXImpl].setExpression1(new FastProperty(1))
      filter.evaluate(sf)
      filter.evaluate(sf)
      success
    }
    "support namespaces" >> {
      val attributeBuilder = new AttributeTypeBuilder()
      attributeBuilder.setBinding(classOf[String])
      val nameType = attributeBuilder.buildType()
      val nameAttribute = attributeBuilder.buildDescriptor(new NameImpl("testns", "name"), nameType)
      val builder = new SimpleFeatureTypeBuilder()
      builder.add(nameAttribute)
      builder.add("geom", classOf[Point], org.locationtech.geomesa.utils.geotools.CRS_EPSG_4326)
      builder.setName("FastPropertyNsTest")
      val sft = builder.buildFeatureType()

      val sf = new ScalaSimpleFeature(sft, "id")
      sf.setAttributes(Array[AnyRef]("myname", "POINT(45 45)"))

      val filter = FastFilterFactory.toFilter(sft, "testns:name = 'myname'")
      filter.evaluate(sf) must beTrue
    }
  }
}