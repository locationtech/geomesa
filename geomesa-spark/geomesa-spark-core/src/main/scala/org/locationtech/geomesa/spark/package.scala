/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa

import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.Try

package object spark {
  trait Schema {
    def schema: SimpleFeatureType
  }

  def isUsingSedona: Boolean = haveSedona && SystemProperty("geomesa.use.sedona").toBoolean.getOrElse(true)

  val haveSedona: Boolean = Try(Class.forName("org.apache.spark.sql.sedona_sql.UDT.GeometryUDT")).isSuccess
}
