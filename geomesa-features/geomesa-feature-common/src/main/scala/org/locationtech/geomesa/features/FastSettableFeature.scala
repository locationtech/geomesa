/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features

import org.geotools.api.feature.simple.SimpleFeature

/**
 * Trait for setting attributes without type conversion, for cases where the attribute types being set have already been
 * validated
 */
trait FastSettableFeature extends SimpleFeature {

  /**
   * Sets the feature ID
   *
   * @param id id
   */
  def setId(id: String): Unit

  /**
   * Set an attribute without using the normal conversion checks. Ensure that the attribute type matches the expected
   * binding, or errors will occur.
   *
   * @param index index of the attribute to set
   * @param value value to set
   */
  def setAttributeNoConvert(index: Int, value: AnyRef): Unit
}
