/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
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
