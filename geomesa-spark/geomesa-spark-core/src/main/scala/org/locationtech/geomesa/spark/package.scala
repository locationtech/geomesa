/***********************************************************************
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa

import org.opengis.feature.simple.SimpleFeatureType

package object spark {

  trait Schema {
    def schema: SimpleFeatureType
  }

  val haveSedona: Boolean = org.locationtech.geomesa.spark.jts.SedonaGeometryUDT.isSuccess

  def isUsingSedona: Boolean = org.locationtech.geomesa.spark.jts.useSedonaSerialization
}
