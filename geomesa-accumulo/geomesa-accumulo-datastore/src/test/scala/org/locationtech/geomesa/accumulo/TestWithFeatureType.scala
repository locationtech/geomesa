/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo

import org.geotools.data.Query
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes

/**
 * Trait to simplify tests that require reading and writing features from an AccumuloDataStore
 */
trait TestWithFeatureType extends TestWithDataStore {

  def spec: String

  lazy val sft = {
    // we use class name to prevent spillage between unit tests
    ds.createSchema(SimpleFeatureTypes.createType(getClass.getSimpleName, spec))
    ds.getSchema(getClass.getSimpleName) // reload the sft from the ds to ensure all user data is set properly
  }

  lazy val sftName = sft.getTypeName

  lazy val fs = ds.getFeatureSource(sftName)

  /**
   * Deletes all existing features
   */
  def clearFeatures(): Unit = clearFeatures(sftName)

  def explain(filter: String): String = explain(new Query(sftName, ECQL.toFilter(filter)))
}
