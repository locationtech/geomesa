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

package org.locationtech.geomesa.accumulo.index

import org.locationtech.geomesa.utils.stats.Cardinality
import org.locationtech.geomesa.utils.stats.Cardinality.Cardinality
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.SimpleFeatureType

/**
 * Provides hints for a simple feature type
 */
trait StrategyHintsProvider {

  /**
   * Get a hint implementation based on the feature type
   *
   * @param sft
   * @return
   */
  def strategyHints(sft: SimpleFeatureType): StrategyHints
}

/**
 * Provides hints for determining query strategies
 */
trait StrategyHints {

  /**
   * Returns the cardinality for an attribute
   *
   * @param ad
   * @return
   */
  def cardinality(ad: AttributeDescriptor): Cardinality
}

/**
 * Implementation of hints that uses user data stored in the attribute descriptor
 */
class UserDataStrategyHints extends StrategyHints {

  import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

  override def cardinality(ad: AttributeDescriptor) = ad.getCardinality()
}

/**
 * No-op implementation of hints, for when you don't care about costs.
 */
object NoOpHints extends StrategyHints {

  override def cardinality(ad: AttributeDescriptor) = Cardinality.UNKNOWN
}