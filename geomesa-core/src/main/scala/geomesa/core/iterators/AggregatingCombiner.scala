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
import org.apache.accumulo.core.iterators.Combiner

/**
 * Responsible for reconstructing the full set of (attribute, value) pairs
 * from the set of all matching keys.
 */
class AggregatingCombiner extends Combiner {
  override def reduce(key: Key, iter: java.util.Iterator[Value]): Value = {
    // there should be exactly one encoded feature per key
    // (until we restore the dispersion)
    iter.hasNext match {
      case true => new Value(decodeAttributeValue(iter.next()).value.getBytes)
      case false => throw new Exception("No values found for combiner")
    }
  }
}
