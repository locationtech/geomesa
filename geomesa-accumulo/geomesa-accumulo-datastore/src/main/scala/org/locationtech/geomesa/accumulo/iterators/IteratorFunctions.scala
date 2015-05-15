/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
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

package org.locationtech.geomesa.accumulo.iterators

import org.apache.accumulo.core.data.{Key, Value}

/**
 * Functions optimized for a single execution path
 */
sealed trait IteratorFunctions extends HasSourceIterator {
  protected val reusableValue = new Value()
}

trait SetTopInclude extends IteratorFunctions {

  /**
   * no eval, just return value
   *
   * @param key
   */
  def setTopInclude(key: Key): Unit = {
    topKey = key
    topValue = source.getTopValue
  }
}

trait SetTopUnique extends SetTopInclude with HasInMemoryDeduplication {

  /**
   * eval uniqueness
   *
   * @param key
   */
  def setTopUnique(key: Key): Unit =
    if (checkUniqueId(key.getColumnQualifier.toString)) { setTopInclude(key) }

}

trait SetTopTransform extends IteratorFunctions with HasFeatureDecoder with HasTransforms {

  /**
   * decode and encode to apply transform
   *
   * @param key
   */
  def setTopTransform(key: Key): Unit = {
    val sf = featureDecoder.deserialize(source.getTopValue.get)
    reusableValue.set(transform(sf))
    topKey = key
    topValue = reusableValue
  }
}

trait SetTopTransformUnique extends SetTopTransform with HasInMemoryDeduplication {


  /**
   * decode and encode to apply transform
   *
   * @param key
   */
  def setTopTransformUnique(key: Key): Unit =
    if (checkUniqueId(key.getColumnQualifier.toString)) { setTopTransform(key) }
}

trait SetTopFilter extends IteratorFunctions with HasFeatureDecoder with HasFilter {

  /**
   * decode to eval filter
   *
   * @param key
   */
  def setTopFilter(key: Key): Unit = {
    val value = source.getTopValue
    val sf = featureDecoder.deserialize(value.get)
    if (filter.evaluate(sf)) {
      topKey = key
      topValue = value
    }
  }
}

trait SetTopFilterUnique extends SetTopFilter with HasInMemoryDeduplication {

  /**
   * decode to eval filter
   *
   * @param key
   */
  def setTopFilterUnique(key: Key): Unit =
    if (checkUniqueId(key.getColumnQualifier.toString)) { setTopFilter(key) }
}

trait SetTopFilterTransform extends IteratorFunctions with HasFeatureDecoder with HasFilter with HasTransforms {


  /**
   * decode to eval filter, encode to apply transform
   *
   * @param key
   */
  def setTopFilterTransform(key: Key): Unit = {
    val sf = featureDecoder.deserialize(source.getTopValue.get)
    if (filter.evaluate(sf)) {
      reusableValue.set(transform(sf))
      topKey = key
      topValue = reusableValue
    }
  }
}

trait SetTopFilterTransformUnique extends SetTopFilterTransform with HasInMemoryDeduplication {

  /**
   * decode to eval filter, encode to apply transform
   *
   * @param key
   */
  def setTopFilterTransformUnique(key: Key): Unit =
    if (checkUniqueId(key.getColumnQualifier.toString)) { setTopFilterTransform(key) }
}

trait SetTopIndexInclude
    extends IteratorFunctions
    with HasIndexValueDecoder
    with HasFeatureDecoder {

  /**
   * for index iterator - no eval, just return value
   *
   * @param key
   */
  def setTopIndexInclude(key: Key): Unit = {
    val sf = indexEncoder.decode(source.getTopValue.get)
    reusableValue.set(featureEncoder.serialize(sf))
    topKey = key
    topValue = reusableValue
  }
}

trait SetTopIndexUnique extends SetTopIndexInclude with HasInMemoryDeduplication {

  /**
   * for index iterator - eval uniqueness
   *
   * @param key
   */
  def setTopIndexUnique(key: Key): Unit =
    if (checkUniqueId(key.getColumnQualifier.toString)) { setTopIndexInclude(key) }
}

trait SetTopIndexFilter
    extends IteratorFunctions
    with HasIndexValueDecoder
    with HasSpatioTemporalFilter
    with HasFeatureDecoder {

  /**
   * for index iterator - decode to eval filter
   *
   * @param key
   */
  def setTopIndexFilter(key: Key): Unit = {
    // the value contains the full-resolution geometry and time plus feature ID
    val sf = indexEncoder.decode(source.getTopValue.get)
    if (stFilter.evaluate(sf)) {
      reusableValue.set(featureEncoder.serialize(sf))
      topKey = key
      topValue = reusableValue
    }
  }
}

trait SetTopIndexFilterUnique extends SetTopIndexFilter with HasInMemoryDeduplication {

  /**
   * decode to eval filter
   *
   * @param key
   */
  def setTopIndexFilterUnique(key: Key): Unit =
    if (checkUniqueId(key.getColumnQualifier.toString)) { setTopIndexFilter(key) }
}

trait SetTopIndexTransform
    extends IteratorFunctions
    with HasIndexValueDecoder
    with HasTransforms {

  /**
   * for index iterator - decode and encode to apply transform
   *
   * @param key
   */
  def setTopIndexTransform(key: Key): Unit = {
    // the value contains the full-resolution geometry and time plus feature ID
    val sf = indexEncoder.decode(source.getTopValue.get)
    reusableValue.set(transform(sf))
    topKey = key
    topValue = reusableValue
  }
}

trait SetTopIndexTransformUnique extends SetTopIndexTransform with HasInMemoryDeduplication {

  /**
   * for index iterator - decode and encode to apply transform
   *
   * @param key
   */
  def setTopIndexTransformUnique(key: Key): Unit =
    if (checkUniqueId(key.getColumnQualifier.toString)) {setTopIndexTransform(key)}
}

trait SetTopIndexFilterTransform
    extends IteratorFunctions
    with HasIndexValueDecoder
    with HasSpatioTemporalFilter
    with HasTransforms {

  /**
   * for index iterator - decode to eval filter, encode to apply transform
   *
   * @param key
   */
  def setTopIndexFilterTransform(key: Key): Unit = {
    // the value contains the full-resolution geometry and time plus feature ID
    val sf = indexEncoder.decode(source.getTopValue.get)
    if (stFilter.evaluate(sf)) {
      reusableValue.set(transform(sf))
      topKey = key
      topValue = reusableValue
    }
  }
}

trait SetTopIndexFilterTransformUnique extends SetTopIndexFilterTransform with HasInMemoryDeduplication {

  /**
   * for index iterator - decode to eval filter, encode to apply transform
   *
   * @param key
   */
  def setTopIndexFilterTransformUnique(key: Key): Unit =
    if (checkUniqueId(key.getColumnQualifier.toString)) { setTopIndexFilterTransform(key) }
}