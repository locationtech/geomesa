/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.text

import com.typesafe.config.ConfigFactory
import org.junit.runner.RunWith
import org.locationtech.geomesa.convert.SimpleFeatureConverters
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CompositeTextConverterTest extends Specification {

  val data =
    """
      |1  ,hello,badvalue,45.0
      |asfastofail,f
      |3
      |2  ,world,,90.0
    """.stripMargin

  val conf = ConfigFactory.parseString(
    """
      | {
      |   type         = "composite-converter",
      |   converters = [
      |     { converter = "first",   predicate = "strEq('1', trim(substr($0, 0, 2)))" },
      |     { converter = "second",  predicate = "strEq('2', trim(substr($0, 0, 2)))" }
      |   ]
      |   first = {
      |     type         = "delimited-text",
      |     format       = "DEFAULT",
      |     id-field     = "concat('first', trim($1))",
      |     fields = [
      |       { name = "phrase", transform = "concat($1, $2)"                    }
      |       { name = "lineNr", transform = "lineNo()"                          }
      |       { name = "lat",    transform = "stringToDouble($3, '0.0'::double)" }
      |       { name = "lon",    transform = "stringToDouble($4, '0.0'::double)" }
      |       { name = "geom",   transform = "point($lat, $lon)"                 }
      |     ]
      |   }
      |
      |   second = {
      |     type         = "delimited-text",
      |     format       = "DEFAULT",
      |     id-field     = "concat('second', trim($1))",
      |     fields = [
      |       { name = "phrase", transform = "concat($1, $2)"                    }
      |       { name = "lat",    transform = "stringToDouble($3, '0.0'::double)" }
      |       { name = "lon",    transform = "stringToDouble($4, '0.0'::double)" }
      |       { name = "geom",   transform = "point($lat, $lon)"                 }
      |       { name = "lineNr", transform = "lineNo()"                          }
      |     ]
      |   }
      | }
    """.stripMargin)

  "be built from a conf" >> {
    val sft = SimpleFeatureTypes.createType(ConfigFactory.load("sft_testsft.conf"))
    val converter = SimpleFeatureConverters.build[String](sft, conf)
    converter must not beNull

    val res = converter.processInput(data.split("\n").toIterator.filterNot( s => "^\\s*$".r.findFirstIn(s).size > 0)).toList

    "and process some data" >> {
      res.size must be equalTo 2
      res(0).getID must be equalTo "first1"
      res(1).getID must be equalTo "second2"
    }

    "and get correct line numbers" >> {
      res(0).getAttribute("lineNr").asInstanceOf[Long] must be equalTo 1
      res(1).getAttribute("lineNr").asInstanceOf[Long] must be equalTo 4
    }

    "testing string2 function defaults" >> {
      res(0).getAttribute("lat").asInstanceOf[Double] must be equalTo 0.0
      res(1).getAttribute("lat").asInstanceOf[Double] must be equalTo 0.0
    }
  }

}
