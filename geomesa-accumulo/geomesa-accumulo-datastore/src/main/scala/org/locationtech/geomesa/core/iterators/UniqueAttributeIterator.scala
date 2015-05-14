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

package org.locationtech.geomesa.core.iterators

import java.util

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.data._
import org.apache.accumulo.core.iterators.{IteratorEnvironment, WrappingIterator}

/**
 * This Iterator should wrap the AttributeIndexIterator. It will skip duplicate attributes to optimize
 * unique queries.
 */
class UniqueAttributeIterator extends WrappingIterator with Logging {

  var seekColFamilies: util.Collection[ByteSequence] = null
  var seekInclusive: Boolean = false
  var seekRange: Range = null
  var outOfRange = false

  override def next() = {
    // skip to the next row key - this will skip over any duplicate attribute values
    val following = new Range(Range.followingPrefix(getTopKey.getRow), true, null, false)
    val range = Option(seekRange.clip(following))
    range match {
      case Some(r) => super.seek(r, seekColFamilies, seekInclusive)
      case None    => outOfRange = true
    }
  }

  override def hasTop() :Boolean = !outOfRange && super.hasTop()

  override def seek(range: Range, columnFamilies: util.Collection[ByteSequence], inclusive: Boolean) {
    // keep track of the current range we're handling
    outOfRange = false
    seekRange = range
    seekColFamilies = columnFamilies
    seekInclusive = inclusive
    super.seek(range, columnFamilies, inclusive)
  }

  override def deepCopy(env: IteratorEnvironment) =
    throw new UnsupportedOperationException("UniqueAttributeIterator does not support deepCopy.")
}


