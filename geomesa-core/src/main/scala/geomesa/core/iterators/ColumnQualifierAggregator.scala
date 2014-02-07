/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geomesa.core.iterators

import SpatioTemporalIntersectingIterator._
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.hadoop.io.Text

trait ColumnQualifierAggregator {
  def collect(key: Key, value: Value)
  def reset()
  def aggregate(): Value
}

class AttributeAggregator extends ColumnQualifierAggregator {
  var attrs = collection.mutable.ListBuffer[Attribute]()

  def reset() {
    attrs.clear()
  }

  def collect(key: Key, value: Value) {
    // the key is not used in this version; the value is a composite that
    // includes the attribute name as well as the attribute value
    attrs += decodeAttributeValue(value)
  }

  def aggregate() = {
    AttributeAggregator.encode(attrs)
  }
}

object AttributeAggregator {
  val SIMPLE_FEATURE_ATTRIBUTE_NAME = "SimpleFeatureAttribute"
  lazy val SIMPLE_FEATURE_ATTRIBUTE_NAME_TEXT =
    new Text(SIMPLE_FEATURE_ATTRIBUTE_NAME)

  def encode(attrs: Seq[Attribute]): Value = {
    // it should not be possible to have more than one attribute per feature
    // (until we re-decompose them in a subsequent round of refactoring)

    assert(attrs.size == 1)

    new Value(attrs.head.value.getBytes)
  }

  def decode(v: Value): Map[String,String] = {
    Map[String,String](SIMPLE_FEATURE_ATTRIBUTE_NAME -> v.toString)
  }
}
