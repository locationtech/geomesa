/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/


package org.locationtech.geomesa.filter.function

import org.geotools.data.DataUtilities
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.feature.{AttributeTypeBuilder, NameImpl}
import org.geotools.filter.spatial.BBOXImpl
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.jts.geom.Point
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FastPropertyTest extends Specification {

  "FastProperty" should {
    "evaluate" >> {
      val filter = ECQL.toFilter("bbox(geom,-180,-90,180,90)")
      val sft = SimpleFeatureTypes.createType("FastPropertyTest", "name:String,*geom:Point:srid=4326")
      val sf = DataUtilities.parse(sft, "id", "myname", "POINT(45 45)")
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

      val sf = DataUtilities.parse(sft, "id", "myname", "POINT(45 45)")

      val filter = FastFilterFactory.toFilter(sft, "testns:name = 'myname'")
      filter.evaluate(sf) must beTrue
    }
  }
}