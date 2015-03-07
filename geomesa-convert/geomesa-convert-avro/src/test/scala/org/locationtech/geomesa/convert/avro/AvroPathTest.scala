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

import org.apache.avro.generic._
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class AvroPathTest extends Specification with AvroUtils {

  sequential

  "AvroPath" should {
    "select a top level path" in {
      val path = "/content"
      val avroPath = AvroPath(path)
      val result = avroPath.eval(gr1)
      val gr = result.get.asInstanceOf[GenericRecord]
      gr.getSchema.getName mustEqual "TObj"
    }

    "select from a union by schema type" in {
      val path = "/content$type=TObj"
      val avroPath = AvroPath(path)
      val result = avroPath.eval(gr1)
      val gr = result.get.asInstanceOf[GenericRecord]
      gr.getSchema.getName mustEqual "TObj"
    }

    "return None when element in union has wrong type" in {
      val path = "/content$type=TObj"
      val avroPath = AvroPath(path)
      val result = avroPath.eval(gr2)
      result.isDefined mustEqual false
    }

    "return nested records" in {
      val path = "/content$type=TObj/kvmap"
      val avroPath = AvroPath(path)
      val result = avroPath.eval(gr1)
      result.isDefined mustEqual true
      val arr = result.get.asInstanceOf[GenericArray[GenericRecord]]
      arr.length mustEqual 4
    }

    "filter arrays of records by a field predicate" in {
      val path = "/content$type=TObj/kvmap[$k=lat]"
      val avroPath = AvroPath(path)
      val result = avroPath.eval(gr1)
      result.isDefined mustEqual true
      val r = result.get.asInstanceOf[GenericRecord]
      r.get("v").asInstanceOf[Double] mustEqual 45.0
    }

    "select a property out of a record in an array" in {
      "filter arrays of records by a field predicate" in {
        val path = "/content$type=TObj/kvmap[$k=lat]/v"
        val avroPath = AvroPath(path)
        val result = avroPath.eval(gr1)
        result.isDefined mustEqual true
        val v = result.get.asInstanceOf[Double]
        v mustEqual 45.0
      }
    }
  }
}
