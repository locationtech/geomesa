/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo

import java.util.concurrent.atomic.AtomicInteger

import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType

/**
 * Trait to simplify tests that require reading and writing features from an AccumuloDataStore
 */
trait TestWithMultipleSfts extends TestWithDataStore {

  private val sftCounter = new AtomicInteger(0)

  /**
   * Create a new schema
   *
   * @param spec simple feature type spec
   * @return
   */
  def createNewSchema(spec: String): SimpleFeatureType = {
    val sftName = s"${getClass.getSimpleName}${sftCounter.getAndIncrement()}"
    ds.createSchema(SimpleFeatureTypes.createType(sftName, spec))
    ds.getSchema(sftName) // reload the sft from the ds to ensure all user data is set properly
  }
}
