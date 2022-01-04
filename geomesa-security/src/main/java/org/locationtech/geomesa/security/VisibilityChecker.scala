/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.security

import org.locationtech.geomesa.utils.text.TextTools
import org.opengis.feature.simple.SimpleFeature

/**
 * Checks for visibilities set in a feature's user data
 */
trait VisibilityChecker {
  @throws(classOf[IllegalArgumentException])
  def requireVisibilities(feature: SimpleFeature): Unit = {
    try {
      val vis = feature.getUserData.get(SecurityUtils.FEATURE_VISIBILITY).asInstanceOf[String]
      if (vis == null || TextTools.isWhitespace(vis)) {
        throw new IllegalArgumentException("Feature does not have required visibility")
      }
    } catch {
      case _: ClassCastException => throw new IllegalArgumentException("Feature does not have required visibility")
    }
  }
}
