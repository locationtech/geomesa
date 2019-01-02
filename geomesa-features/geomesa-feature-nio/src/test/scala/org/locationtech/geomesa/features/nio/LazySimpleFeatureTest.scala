/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.nio

import java.nio.ByteBuffer
import java.util

import com.typesafe.scalalogging.LazyLogging
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.nio.AttributeAccessor.ByteBufferSimpleFeatureSerializer
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.languageFeature.postfixOps

@RunWith(classOf[JUnitRunner])
class LazySimpleFeatureTest extends Specification with LazyLogging {

  sequential

  "LazySimpleFeature" should {

    "correctly deserialize basic features" in {
      val spec = "a:Integer,c:Double,d:Long,f:String,g:Boolean,dtg:Date,*geom:Point:srid=4326"
      val sft = SimpleFeatureTypes.createType("testType", spec)

      val sf = new ScalaSimpleFeature(sft, "fakeid")

      sf.setAttribute("a", "1")
      sf.setAttribute("c", "5.37")
      sf.setAttribute("d", "-100")
      sf.setAttribute("f", "mystring")
      sf.setAttribute("g", java.lang.Boolean.FALSE)
      sf.setAttribute("dtg", "2013-01-02T00:00:00.000Z")
      sf.setAttribute("geom", "POINT(45.0 49.0)")

      val writer = new ByteBufferSimpleFeatureSerializer(sft)
      val buf = ByteBuffer.allocate(2048)
      val bytesWritten = writer.write(buf, sf)
      val serialized = util.Arrays.copyOfRange(buf.array(), 0, bytesWritten)
      logger.debug(serialized.length.toString)

      val accessors = AttributeAccessor.buildSimpleFeatureTypeAttributeAccessors(sft)
      val laz = new LazySimpleFeature("fakeid", sft, accessors, ByteBuffer.wrap(serialized))

      laz.getID mustEqual sf.getID
      laz.getAttribute("a") mustEqual sf.getAttribute("a")
      laz.getAttribute("c") mustEqual sf.getAttribute("c")
      laz.getAttribute("d") mustEqual sf.getAttribute("d")
      laz.getAttribute("f") mustEqual sf.getAttribute("f")
      laz.getAttribute("g") mustEqual sf.getAttribute("g")
      laz.getAttribute("dtg") mustEqual sf.getAttribute("dtg")
      laz.getAttribute("geom") mustEqual sf.getAttribute("geom")
    }

    "be faster than full deserialization" in {
      skipped("integration")
      val spec = "a:Integer,c:Double,d:Long,f:String,g:Boolean,dtg:Date,*geom:Point:srid=4326"
      val sft = SimpleFeatureTypes.createType("speed", spec)

      val sf = new ScalaSimpleFeature(sft, "fakeid")

      sf.setAttribute("a", "1")
      sf.setAttribute("c", "5.37")
      sf.setAttribute("d", "-100")
      sf.setAttribute("f", "mystring")
      sf.setAttribute("g", java.lang.Boolean.FALSE)
      sf.setAttribute("dtg", "2013-01-02T00:00:00.000Z")
      sf.setAttribute("geom", "POINT(45.0 49.0)")

      val writer = new ByteBufferSimpleFeatureSerializer(sft)
      val buf = ByteBuffer.allocate(2048)
      val bytesWritten = writer.write(buf, sf)
      val serialized = util.Arrays.copyOfRange(buf.array(), 0, bytesWritten)

      val accessors = AttributeAccessor.buildSimpleFeatureTypeAttributeAccessors(sft)

      val start2 = System.currentTimeMillis()
      val reusable = new LazySimpleFeature("fakeid", sft, accessors, ByteBuffer.wrap(serialized))
      (0 until 1000000).foreach { _ =>
        reusable.setBuf(ByteBuffer.wrap(serialized))
        reusable.getAttribute(6)
      }
      logger.debug(s"took ${System.currentTimeMillis() - start2}ms\n\n")

      success
    }
  }
}
