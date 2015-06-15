/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

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