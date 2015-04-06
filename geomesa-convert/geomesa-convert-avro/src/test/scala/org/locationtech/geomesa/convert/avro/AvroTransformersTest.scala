/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.convert.avro

import org.junit.runner.RunWith
import org.locationtech.geomesa.convert.Transformers
import org.locationtech.geomesa.convert.Transformers.EvaluationContext
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.mutable

@RunWith(classOf[JUnitRunner])
class AvroTransformersTest extends Specification with AvroUtils {

  sequential

  "Transformers" should {
    implicit val ctx = new EvaluationContext(mutable.HashMap.empty[String, Int], null)
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
