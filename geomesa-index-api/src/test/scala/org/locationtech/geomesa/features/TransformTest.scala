/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features

import org.geotools.feature.simple.SimpleFeatureBuilder
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.Transform.Transforms
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TransformTest extends Specification {

  "Transforms" should {
    "extract json path transforms" in {
      val sft =
        SimpleFeatureTypes.createType("test",
          "name:String,team:String,age:Int,props:String:json=true,dtg:Date,*geom:Point:srid=4326")
      val sf = SimpleFeatureBuilder.build(sft, Array.empty[AnyRef], "0")
      Seq("name0", "team0", 20, """{"color":"blue"}""", "2017-02-03T00:00:01.000Z", "POINT(40 60)")
          .zipWithIndex.foreach { case (a, i) => sf.setAttribute(i, a)}
      val transforms = Transforms(sft, Seq("name", "color=\"$.props.color\"", "dtg", "geom"))
      transforms.map(_.evaluate(sf)) mustEqual Seq("name0", "blue", sf.getAttribute("dtg"), sf.getAttribute("geom"))
    }
  }
}
