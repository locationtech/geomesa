/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AttributeIndexTest extends Specification {

  "AttributeIndex" should {
    "convert shorts to bytes and back" in {
      forall(Seq(0, 32, 127, 128, 129, 255, 256, 257)) { i =>
        val bytes = AttributeIndex.indexToBytes(i)
        bytes must haveLength(2)
        val recovered = AttributeIndex.bytesToIndex(bytes(0), bytes(1))
        recovered mustEqual i
      }
    }
  }
}
