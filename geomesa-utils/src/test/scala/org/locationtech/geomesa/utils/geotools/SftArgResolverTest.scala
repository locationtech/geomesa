/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SftArgResolverTest extends Specification {

  "SftArgResolver" should {
    "resolve substitutions in provided config strings and support specifying the feature type by name" >> {
      val spec =
        """
          |geomesa.sfts : {
          |  "common-base" : {
          |    "attributes" : [
          |      { "name" : "foo", "type" : "String" },
          |      { "name" : "dtg", "type" : "Date" },
          |    ]
          |  },
          |  "points" : ${geomesa.sfts.common-base} {
          |    "type-name" : "points",
          |    "attributes" :  ${geomesa.sfts.common-base.attributes} [
          |      { "name" : "geom", "type" : "Point", "srid" : "4326", "default" : "true" }
          |    ]
          |  },
          |  "lines" : ${geomesa.sfts.common-base} {
          |    "type-name" : "lines",
          |    "attributes" :  ${geomesa.sfts.common-base.attributes} [
          |      { "name" : "geom", "type" : "LineString", "srid" : "4326", "default" : "true" }
          |    ]
          |  }
          |}
          |""".stripMargin
      val points = SftArgResolver.getArg(SftArgs(spec, "points"))
      points must beRight(SimpleFeatureTypes.createType("points", "foo:String,dtg:Date,*geom:Point:srid=4326"))
      val lines = SftArgResolver.getArg(SftArgs(spec, "lines"))
      lines must beRight(SimpleFeatureTypes.createType("lines", "foo:String,dtg:Date,*geom:LineString:srid=4326"))
    }
  }
}
