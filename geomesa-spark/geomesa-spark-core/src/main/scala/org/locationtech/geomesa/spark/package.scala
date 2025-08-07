/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa

import org.geotools.api.feature.simple.SimpleFeatureType

package object spark {

  trait Schema {
    def schema: SimpleFeatureType
  }

  val haveSedona: Boolean = org.locationtech.geomesa.spark.jts.SedonaGeometryUDT.isSuccess

  def isUsingSedona: Boolean = org.locationtech.geomesa.spark.jts.useSedonaSerialization
}
