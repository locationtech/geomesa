/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.csv

import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.csv.DMS._
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DMSTest extends Specification {
  "Hemisphere" should {
    "recognize all valid characters" >> {
      Hemisphere('N') mustEqual North
      Hemisphere('n') mustEqual North
      Hemisphere('S') mustEqual South
      Hemisphere('s') mustEqual South
      Hemisphere('E') mustEqual East
      Hemisphere('e') mustEqual East
      Hemisphere('W') mustEqual West
      Hemisphere('w') mustEqual West
    }

    "reject invalid characters" >> {
      Hemisphere('Q') must throwA[IllegalArgumentException]
    }
  }

  "DMS" should {
    val dms = DMS(38,4,31.17,North)

    "parse DMS strings with colons" >> {
      DMS("38:04:31.17N") mustEqual dms
    }

    "parse DMS strings without colons" >> {
      DMS("380431.17N") mustEqual dms
    }

    "parse DMS strings with signs" >> {
      DMS("-38:04:31.17S") mustEqual dms
    }

    "reject DMS strings with too many seconds" >> {
      DMS("38:04:61.17N") must throwA[IllegalArgumentException]
    }

    "reject DMS strings with too many minutes" >> {
      DMS("38:64:31.17N") must throwA[IllegalArgumentException]
    }

    "reject DMS strings with too many degrees" >> {
      DMS("98:04:61.17N") must throwA[IllegalArgumentException]
    }
  }
}
