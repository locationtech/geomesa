/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.uuid

import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
 * Trait for generating feature ids based on attributes of a simple feature
 */
trait FeatureIdGenerator {

  /**
   * Create a unique feature ID
   */
  def createId(sft: SimpleFeatureType, sf: SimpleFeature): String
}
