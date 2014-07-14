/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
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


package geomesa.core.iterators

import org.apache.accumulo.core.data.{Key, Value}
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DeDuplicatingIteratorTest extends Specification {

  val key = new Key("test")
  val value = new Value(Array[Byte](2.toByte))
  val dedup = new DeDuplicator((key: Key, value: Value) => extractFeatureId(value))
  def extractFeatureId(value: Value): String = {
    val vString = value.toString
    vString
  }

  "DeDuplicatingIterator" should {
    "return true for a unique element" in {
      val bool = dedup.isUnique(key, value)

      dedup.close()

      bool must beTrue
    }

    "return true for a duplicate element" in {
      val unique = dedup.isDuplicate(key, value)

      val duplicate = dedup.isDuplicate(key, value)

      dedup.close()

      unique must beFalse
      duplicate must beTrue
    }
  }
}