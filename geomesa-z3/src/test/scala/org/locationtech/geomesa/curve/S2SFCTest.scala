/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.curve

import org.junit.runner.RunWith
import org.locationtech.sfcurve.CoveredRange
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class S2SFCTest extends Specification {

  lazy val sfc = S2SFC(0, 30, 1, 8)

  "S2" should {
    "calculate ranges" >> {
      val ranges = sfc.ranges(34.9 -> 45.1, 54.9 -> 75.1)
      ranges must haveLength(7)
      ranges must containTheSameElementsAs(
        Seq(
          CoveredRange(4683743612465315841L,4719772409484279807L),
          CoveredRange(4899914195555844097L,4899916394579099647L),
          CoveredRange(4899916394579099649L,4935945191598063615L),
          CoveredRange(5017009984890732545L,5026017184145473535L),
          CoveredRange(5088926841440305153L,5089067578928660479L),
          CoveredRange(5089067578928660481L,5098074778183401471L),
          CoveredRange(5100326577997086721L,5102578377810771967L)
        )
      )
    }
    "calculate more ranges" >> {
      val ranges = sfc.ranges(35d -> 45d, 55d -> 75d)
      ranges must haveLength(7)
      ranges must containTheSameElementsAs(
        Seq(
          CoveredRange(4683743612465315841L,4719772409484279807L),
          CoveredRange(4899916394579099647L,4899916394579099647L),
          CoveredRange(4899916394579099649L,4935945191598063615L),
          CoveredRange(5017009984890732545L,5026017184145473535L),
          CoveredRange(5089032394556571649L,5089067578928660479L),
          CoveredRange(5089067578928660481L,5098074778183401471L),
          CoveredRange(5100326577997086721L,5102578377810771967L)
        )
      )
    }
  }
}
