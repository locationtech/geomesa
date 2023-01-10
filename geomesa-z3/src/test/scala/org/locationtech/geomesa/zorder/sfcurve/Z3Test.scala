/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 1463162d60 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 9f430502b2 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> dce8c58b44 (GEOMESA-3254 Add Bloop build support)
 * Copyright (c) 2015 Azavea.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.zorder.sfcurve

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class Z3Test extends Specification {
  "Z3 encoding" should {
    "interlaces bits" in {
      // (x,y,z) - x has the lowest sigfig bit
      Z3(1,0,0).z mustEqual 1
      Z3(0,1,0).z mustEqual 2
      Z3(0,0,1).z mustEqual 4
      Z3(1,1,1).z mustEqual 7
    }

    "deinterlaces bits" in {
      Z3(23,13,200).decode mustEqual(23, 13, 200)

      //only 21 bits are saved, so Int.MaxValue is CHOPPED
      Z3(Int.MaxValue, 0, 0).decode mustEqual(2097151, 0, 0)
      Z3(Int.MaxValue, 0, Int.MaxValue).decode mustEqual(2097151, 0, 2097151)
    }

    "unapply" in{
      val Z3(x,y,z) = Z3(3,5,1)
      x mustEqual 3
      y mustEqual 5
      z mustEqual 1
    }
  }
}
