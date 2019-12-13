/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.stats

import org.geotools.feature.simple.SimpleFeatureBuilder
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes

trait StatTestHelper {
  val sftSpec = "strAttr:String,intAttr:Integer,longAttr:Long,doubleAttr:Double,floatAttr:Float,cat1:Integer,cat2:String,geom:Geometry:srid=4326,dtg:Date"
  val sft = SimpleFeatureTypes.createType("test", sftSpec)

  val features = (0 until 100).toArray.map { i =>
    val a = Array(f"abc$i%03d", i, i, i, i, i%10, String.valueOf((math.abs(i%26) + 'A').toChar), s"POINT(-$i ${i / 2})", f"2012-01-01T${i%24}%02d:00:00.000Z")
    SimpleFeatureBuilder.build(sft, a.asInstanceOf[Array[AnyRef]], i.toString)
  }

  val features2 = (100 until 200).toArray.map { i =>
    val a = Array(f"abc$i%03d", i, i, i, i, i%10, String.valueOf((math.abs(i%26) + 'A').toChar),s"POINT(${i -20} ${i / 2 - 20})", f"2012-01-02T${i%24}%02d:00:00.000Z")
    SimpleFeatureBuilder.build(sft, a.asInstanceOf[Array[AnyRef]], i.toString)
  }

  val features3 = (-100 until 0).toArray.map { i =>
    val a = Array(f"abc$i%+03d", i, i, i, i, i%10, String.valueOf((math.abs(i%26) + 'A').toChar),s"POINT($i $i)", f"2012-01-02T${Math.abs(i)%24}%02d:00:00.000Z")
    SimpleFeatureBuilder.build(sft, a.asInstanceOf[Array[AnyRef]], i.toString)
  }

  implicit class WhiteSpace(s: String) {
    def ignoreSpace: String = s.replaceAll("\\s+","\\\\s*")
  }
}