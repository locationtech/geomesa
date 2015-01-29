/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.convert.fixedwidth

import com.typesafe.config.ConfigFactory
import com.vividsolutions.jts.geom.{Coordinate, Point}
import org.junit.runner.RunWith
import org.locationtech.geomesa.convert.SimpleFeatureConverters
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FixedWidthConverterTest extends Specification {

  "FixedWidthConverter" >> {
    val conf = ConfigFactory.parseString(
      """
        | converter = {
        |   type      = "fixed-width"
        |   type-name = "testsft"
        |   id-field  = "uuid()"
        |   fields = [
        |     { name = "lat",  transform = "$0::double", start = 1, width = 2 },
        |     { name = "lon",  transform = "$0::double", start = 3, width = 2 },
        |     { name = "geom", transform = "point($lon, $lat)" }
        |   ]
        | }
      """.stripMargin)

    val sft = SimpleFeatureTypes.createType(ConfigFactory.load("sft_testsft.conf"))
    val converter = SimpleFeatureConverters.build[String](sft, conf)

    "process fixed with data" >> {
      val data =
        """
          |14555
          |16565
        """.stripMargin

      converter must not beNull
      val res = converter.processInput(data.split("\n").toIterator.filterNot( s => "^\\s*$".r.findFirstIn(s).size > 0)).toList
      res.size must be equalTo 2
      res(0).getDefaultGeometry.asInstanceOf[Point].getCoordinate must be equalTo new Coordinate(55.0, 45.0)
      res(1).getDefaultGeometry.asInstanceOf[Point].getCoordinate must be equalTo new Coordinate(65.0, 65.0)
    }
  }
}
