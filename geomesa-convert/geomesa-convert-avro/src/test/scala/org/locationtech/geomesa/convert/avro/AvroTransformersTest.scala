/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.avro

import org.junit.runner.RunWith
import org.locationtech.geomesa.convert.{EvaluationContext, Transformers}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AvroTransformersTest extends Specification with AvroUtils {

  sequential

  "Transformers" should {
    implicit val ctx = EvaluationContext.empty
    "handle Avro records" >> {

      "extract an inner value" >> {
        val exp = Transformers.parseTransform("avroPath($0, '/content$type=TObj/kvmap[$k=prop3]/v')")
        exp.eval(Array(decoded)) must be equalTo " foo "
      }

      "handle compound expressions" >> {
        val exp = Transformers.parseTransform("trim(avroPath($0, '/content$type=TObj/kvmap[$k=prop3]/v'))")
        exp.eval(Array(decoded)) must be equalTo "foo"
      }
    }

  }
}
