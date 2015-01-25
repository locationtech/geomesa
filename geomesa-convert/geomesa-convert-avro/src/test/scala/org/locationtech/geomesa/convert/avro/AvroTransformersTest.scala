package org.locationtech.geomesa.convert.avro

import org.junit.runner.RunWith
import org.locationtech.geomesa.convert.Transformers
import org.locationtech.geomesa.convert.Transformers.EvaluationContext
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AvroTransformersTest extends Specification with AvroUtils {

  sequential

  "Transformers" should {
    implicit val ctx = new EvaluationContext(Map(), null)
    "handle Avro records" >> {

      "extract an inner value" >> {
        val exp = Transformers.parseTransform("avroPath($0, '/content$type=TObj/kvmap[$k=prop3]/v')")
        exp.eval(decoded) must be equalTo " foo "
      }

      "handle compound expressions" >> {
        val exp = Transformers.parseTransform("trim(avroPath($0, '/content$type=TObj/kvmap[$k=prop3]/v'))")
        exp.eval(decoded) must be equalTo "foo"
      }
    }

  }
}
