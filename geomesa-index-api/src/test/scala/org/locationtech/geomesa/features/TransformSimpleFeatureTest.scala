/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features

import org.geotools.data.Query
import org.junit.runner.RunWith
import org.locationtech.geomesa.index.planning.QueryPlanner
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TransformSimpleFeatureTest extends Specification {

  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  val spec = "name:String,age:Int,dtg:Date,*geom:Point:srid=4326"
  val sft = SimpleFeatureTypes.createType("transform", spec)

  def transformFeature(transforms: Array[String]): TransformSimpleFeature = {
    val query = new Query("transform", Filter.INCLUDE, transforms)
    QueryPlanner.setQueryTransforms(query, sft)
    TransformSimpleFeature(sft, query.getHints.getTransformSchema.get, query.getHints.getTransformDefinition.get)
  }

  "TransformSimpleFeature" should {
    "correctly project existing attributes" in {
      val transform = transformFeature(Array("name", "geom"))
      val sf = ScalaSimpleFeature.create(sft, "0", "name1", "22", "2017-01-01T00:00:00.000Z", "POINT(45 55)")
      transform.setFeature(sf)

      transform.getFeatureType.getAttributeCount mustEqual 2
      transform.getID mustEqual "0"
      transform.getAttribute("name") mustEqual "name1"
      transform.getAttribute(0) mustEqual "name1"
      transform.getAttribute("geom") mustEqual WKTUtils.read("POINT(45 55)")
      transform.getAttribute(1) mustEqual WKTUtils.read("POINT(45 55)")
    }
    "correctly project transform functions" in {
      val transform = transformFeature(Array("name", "geom", "derived=strConcat('hello',name)"))
      val sf = ScalaSimpleFeature.create(sft, "0", "name1", "22", "2017-01-01T00:00:00.000Z", "POINT(45 55)")
      transform.setFeature(sf)

      transform.getFeatureType.getAttributeCount mustEqual 3
      transform.getID mustEqual "0"
      transform.getAttribute("name") mustEqual "name1"
      transform.getAttribute(0) mustEqual "name1"
      transform.getAttribute("geom") mustEqual WKTUtils.read("POINT(45 55)")
      transform.getAttribute(1) mustEqual WKTUtils.read("POINT(45 55)")
      transform.getAttribute("derived") mustEqual "helloname1"
      transform.getAttribute(2) mustEqual "helloname1"
    }
  }
}
