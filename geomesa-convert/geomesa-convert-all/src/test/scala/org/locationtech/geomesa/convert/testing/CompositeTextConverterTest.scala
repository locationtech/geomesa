/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.testing
// this has to be in testing package because of weird import shadowing

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.junit.runner.RunWith
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CompositeTextConverterTest extends Specification with LazyLogging {

  sequential

  val data =
    """1  ,hello,badvalue,45.0
      |asfastofail,f
      |3
      |2  ,world,,90.0
    """.stripMargin

  val localConf = ConfigFactory.parseString(
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

  val sft = SimpleFeatureTypes.createType(ConfigFactory.load("sft_testsft.conf"))

  "CompositeConverter" should {

    "process some data using local conf" in {
      WithClose(SimpleFeatureConverter(sft, localConf)) { converter =>
        converter must not(beNull)

        val res = WithClose(converter.process(new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8))))(_.toList)

        res.size must be equalTo 2
        res(0).getID must be equalTo "first1"
        res(1).getID must be equalTo "second2"

        // and get correct line numbers
        res(0).getAttribute("lineNr").asInstanceOf[Long] must be equalTo 1
        res(1).getAttribute("lineNr").asInstanceOf[Long] must be equalTo 4

        // and get default string to double values
        res(0).getAttribute("lat").asInstanceOf[Double] must be equalTo 0.0
        res(1).getAttribute("lat").asInstanceOf[Double] must be equalTo 0.0
      }
    }

    "reference converters using global conf" in {
      WithClose(SimpleFeatureConverter(sft, "comp1")) { converter =>
        converter must not(beNull)
        val res = WithClose(converter.process(new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8))))(_.toList)

        res.size must be equalTo 2
        res(0).getID must be equalTo "first1"
        res(1).getID must be equalTo "second2"

        // and get correct line numbers
        res(0).getAttribute("lineNr").asInstanceOf[Long] must be equalTo 1
        res(1).getAttribute("lineNr").asInstanceOf[Long] must be equalTo 4

        // and get default string to double values
        res(0).getAttribute("lat").asInstanceOf[Double] must be equalTo 0.0
        res(1).getAttribute("lat").asInstanceOf[Double] must be equalTo 0.0
      }
    }

    "call next without hasNext" in {
      WithClose(SimpleFeatureConverter(sft, "comp1")) { converter =>
        converter must not(beNull)
        val iter = converter.process(new ByteArrayInputStream("3 5050".getBytes))
        try {
          iter.next.getID mustEqual "50.050.0"
        } finally {
          iter.close()
        }
      }
    }

    "be built using old api" in {
      import org.locationtech.geomesa.convert.SimpleFeatureConverters

      val converter = SimpleFeatureConverters.build[String](sft, localConf)
      converter must not(beNull)

      val res = converter.processInput(data.split("\n").iterator).toList

      res.size must be equalTo 2
      res(0).getID must be equalTo "first1"
      res(1).getID must be equalTo "second2"

      // and get correct line numbers
      res(0).getAttribute("lineNr").asInstanceOf[Long] must be equalTo 1
      res(1).getAttribute("lineNr").asInstanceOf[Long] must be equalTo 4

      // and get default string to double values
      res(0).getAttribute("lat").asInstanceOf[Double] must be equalTo 0.0
      res(1).getAttribute("lat").asInstanceOf[Double] must be equalTo 0.0
    }
  }
}
