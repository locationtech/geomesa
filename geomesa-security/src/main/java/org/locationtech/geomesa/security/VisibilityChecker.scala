/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.security

import org.geotools.api.feature.simple.SimpleFeature

/**
 * Checks for visibilities set in a feature's user data
 */
trait VisibilityChecker {
  @throws(classOf[IllegalArgumentException])
  def requireVisibilities(feature: SimpleFeature): Unit = {
    try {
      val vis = feature.getUserData.get(SecurityUtils.FEATURE_VISIBILITY).asInstanceOf[String]
      if (vis == null || vis.isBlank) {
        throw new IllegalArgumentException("Feature does not have required visibility")
      }
    } catch {
      case _: ClassCastException => throw new IllegalArgumentException("Feature does not have required visibility")
    }
  }
}
