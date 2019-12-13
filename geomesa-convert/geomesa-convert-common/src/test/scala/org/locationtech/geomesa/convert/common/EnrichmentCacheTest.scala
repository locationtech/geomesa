/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.convert.common

import com.typesafe.config.ConfigFactory
import org.junit.runner.RunWith
import org.locationtech.geomesa.convert.SimpleFeatureConverters
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class EnrichmentCacheTest extends Specification {

  "EnrichmentCache" should {
    "work" >> {
      val conf = ConfigFactory.parseString(
        """
          | {
          |   type         = "test"
          |   id-field     = "$id"
          |   caches = {
          |      test = {
          |         type = "simple"
          |         data = {
          |             1 = {
          |               name = "foo"
          |             }
          |         }
          |      }
          |   }
          |   fields = [
          |     { name = "id",     transform = "toString($0)"      }
          |     { name = "keytolookup", transform = "cacheLookup('test', $id, 'name')"   }
          |     { name = "lat",    transform = "$1::double"       }
          |     { name = "lon",    transform = "$2::double"       }
          |     { name = "geom",   transform = "point($lon, $lat)" }
          |   ]
          | }
        """.stripMargin
      )

      val data = "1,35.0,35.0"

      val sftConfPoint = ConfigFactory.parseString(
        """{ type-name = "testsft"
          |  attributes = [
          |    { name = "keytolookup", type = "String" }
          |    { name = "geom",   type = "Point"       }
          |  ]
          |}
        """.stripMargin)

      val sft = SimpleFeatureTypes.createType(sftConfPoint)

      val conv = SimpleFeatureConverters.build[String](sft, conf)
      val features = conv.processInput(Iterator.apply(data)).toArray
      features.length must be equalTo 1
      features(0).getAttribute("keytolookup").asInstanceOf[String] must be equalTo "foo"
    }

    "load resource files" >> {
      val conf = ConfigFactory.parseString(
        """
          | {
          |   type         = "test"
          |   id-field     = "$id"
          |   caches = {
          |      test = {
          |         type = "resource"
          |         path = "lookuptable.csv"
          |         columns = ["id", "name", "age"]
          |         id-field = "id"
          |      }
          |   }
          |   fields = [
          |     { name = "id",     transform = "toString($0)"      }
          |     { name = "keytolookup", transform = "cacheLookup('test', $id, 'name')"   }
          |     { name = "lat",    transform = "$1::double"       }
          |     { name = "lon",    transform = "$2::double"       }
          |     { name = "geom",   transform = "point($lon, $lat)" }
          |   ]
          | }
        """.stripMargin
      )

      val data = "1,35.0,35.0"

      val sftConfPoint = ConfigFactory.parseString(
        """{ type-name = "testsft"
          |  attributes = [
          |    { name = "keytolookup", type = "String" }
          |    { name = "geom",   type = "Point"       }
          |  ]
          |}
        """.stripMargin)

      val sft = SimpleFeatureTypes.createType(sftConfPoint)

      val conv = SimpleFeatureConverters.build[String](sft, conf)
      val features = conv.processInput(Iterator.apply(data)).toArray
      features.length must be equalTo 1
      features(0).getAttribute("keytolookup").asInstanceOf[String] must be equalTo "foo"

    }
  }
}
