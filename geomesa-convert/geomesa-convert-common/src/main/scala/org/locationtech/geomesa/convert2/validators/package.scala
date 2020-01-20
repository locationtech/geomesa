/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2

import com.codahale.metrics.Counter
import org.locationtech.geomesa.utils.geotools.wholeWorldEnvelope
import org.opengis.feature.simple.SimpleFeature

package object validators {

  /**
    * Validates an attribute is not null
    *
    * @param i attribute index
    * @param error error message
    * @param counter optional counter for validation failures
    */
  class NullValidator(i: Int, error: String, counter: Counter) extends SimpleFeatureValidator {
    override def validate(sf: SimpleFeature): String = {
      if (sf.getAttribute(i) != null) { null } else {
        counter.inc()
        error
      }
    }
    override def close(): Unit = {}
  }

  object NoValidator extends SimpleFeatureValidator {
    override def validate(sf: SimpleFeature): String = null
    override def close(): Unit = {}
  }

  object Errors {
    val GeomNull   = "geometry is null"
    val DateNull   = "date is null"
    val GeomBounds = s"geometry exceeds world bounds ($wholeWorldEnvelope)"
  }
}
