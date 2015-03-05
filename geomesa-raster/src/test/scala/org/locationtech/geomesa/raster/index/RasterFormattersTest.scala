/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.raster.index

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RasterFormattersTest extends Specification {

  "DoubleTextFormatter" should {
    val testNumber = 123456789
    val doubleFormatter = DoubleTextFormatter(testNumber)

    "map a double to a string representation using only four significant digits" in {
      val scientificText = doubleFormatter.formatString(null, null, null)
      scientificText must be equalTo "c19d6bbd00000000"
    }

    "map a double to a string representation that is equal to that for the same number with four significant digits " in {
      val scientificText = doubleFormatter.formatString(null, null, null)
      val truncatedNumber = 123400000
      val truncatedText = DoubleTextFormatter(truncatedNumber).formatString(null, null, null)

      scientificText must be equalTo truncatedText
    }
  }

  "RasterBandTextFormatter" should {
    val testBand = "RGB"
    val bandTextFormatter = RasterBandTextFormatter(testBand)

    "map a string band name to the same string representation" in {
      val bandText = bandTextFormatter.formatString(null, null, null)
      bandText must be equalTo "RGB"
    }
  }

}
