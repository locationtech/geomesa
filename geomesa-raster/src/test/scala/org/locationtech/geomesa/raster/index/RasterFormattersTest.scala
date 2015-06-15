/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

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

}
