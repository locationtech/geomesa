/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.plugin.ui

import org.junit.runner.RunWith
import org.locationtech.geomesa.plugin.ui.components.Formatting
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FormatTest extends Specification {
  "Formatter" should {
    "format memory" >> {
      Formatting.formatMem(Math.pow(2, 0).toLong) mustEqual "1"
      Formatting.formatMem(Math.pow(2, 10).toLong) mustEqual "1.00KB"
      Formatting.formatMem(Math.pow(2, 20).toLong) mustEqual "1.00MB"
      Formatting.formatMem(Math.pow(2, 30).toLong) mustEqual "1.00GB"
      Formatting.formatMem(Math.pow(2, 40).toLong) mustEqual "1.00TB"
      Formatting.formatMem(Math.pow(2, 50).toLong) mustEqual "1.00PB"
      Formatting.formatMem(Math.pow(2, 60).toLong) mustEqual "1.00EB"

      Formatting.formatMem(1023) mustEqual "1023"
      Formatting.formatMem(1024) mustEqual "1.00KB"
      Formatting.formatMem(54031024) mustEqual "51.53MB"
    }

    "format large numbers" >> {
      Formatting.formatLargeNum(Math.pow(10, 0).toLong) mustEqual "1"
      Formatting.formatLargeNum(Math.pow(10, 3).toLong) mustEqual "1.00K"
      Formatting.formatLargeNum(Math.pow(10, 6).toLong) mustEqual "1.00M"
      Formatting.formatLargeNum(Math.pow(10, 9).toLong) mustEqual "1.00G"
      Formatting.formatLargeNum(Math.pow(10, 12).toLong) mustEqual "1.00T"
      Formatting.formatLargeNum(Math.pow(10, 15).toLong) mustEqual "1.00P"
      Formatting.formatLargeNum(Math.pow(10, 18).toLong) mustEqual "1.00E"

      Formatting.formatLargeNum(999) mustEqual "999"
      Formatting.formatLargeNum(1024) mustEqual "1.02K"
      Formatting.formatLargeNum(54031024) mustEqual "54.03M"
    }
  }
}
