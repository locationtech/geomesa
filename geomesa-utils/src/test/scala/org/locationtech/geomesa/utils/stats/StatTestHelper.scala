/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.stats

import org.geotools.feature.simple.SimpleFeatureBuilder
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes

trait StatTestHelper {
  val sftSpec = "strAttr:String,intAttr:Integer,longAttr:Long,doubleAttr:Double,floatAttr:Float,geom:Geometry:srid=4326,dtg:Date"
  val sft = SimpleFeatureTypes.createType("test", sftSpec)

  val stringIndex = sft.indexOf("strAttr")
  val intIndex = sft.indexOf("intAttr")
  val longIndex = sft.indexOf("longAttr")
  val doubleIndex = sft.indexOf("doubleAttr")
  val floatIndex = sft.indexOf("floatAttr")
  val geomIndex = sft.indexOf("geom")
  val dateIndex = sft.indexOf("dtg")

  val features = (0 until 100).toArray.map { i =>
    val a = Array(f"abc$i%03d", i, i, i, i, s"POINT(-$i ${i / 2})", f"2012-01-01T${i%24}%02d:00:00.000Z")
    SimpleFeatureBuilder.build(sft, a.asInstanceOf[Array[AnyRef]], i.toString)
  }

  val features2 = (100 until 200).toArray.map { i =>
    val a = Array(f"abc$i%03d", i, i, i, i, s"POINT(${i -20} ${i / 2 - 20})", f"2012-01-02T${i%24}%02d:00:00.000Z")
    SimpleFeatureBuilder.build(sft, a.asInstanceOf[Array[AnyRef]], i.toString)
  }
}