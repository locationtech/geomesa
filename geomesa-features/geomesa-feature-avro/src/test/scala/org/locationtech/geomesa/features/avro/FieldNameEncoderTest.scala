/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.avro

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FieldNameEncoderTest extends Specification {

  "Avro FieldNameEncoder" should {
    val nameEncoder = new FieldNameEncoder(4)
    "not encode alphanumerics" >> {
      val original = "alphaSafe423"
      val e = nameEncoder.encode(original)
      e mustEqual original
      nameEncoder.decode(e) mustEqual original
    }

    "encode underscores" >> {
      val original = "_alpha_Safe423_"
      val target = "_5falpha_5fSafe423_5f"
      val e = nameEncoder.encode(original)
      e mustEqual target
      nameEncoder.decode(e) mustEqual original
    }

    "encode other things" >> {
      val original = "_alpha-Safe423%"
      val target = "_5falpha_2dSafe423_25"
      val e = nameEncoder.encode(original)
      e mustEqual target
      nameEncoder.decode(e) mustEqual original
    }
  }

}
