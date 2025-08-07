/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.spark.sql

import org.apache.spark.sql.SQLContext
import org.locationtech.geomesa.spark.isUsingSedona
import org.locationtech.geomesa.spark.jts._
import org.locationtech.geomesa.spark.sedona._

object SQLTypes {

  def init(sqlContext: SQLContext): Unit = {
    initJTS(sqlContext)
    SQLRules.registerOptimizations(sqlContext)
    GeometricDistanceFunctions.registerFunctions(sqlContext)
    if (isUsingSedona) {
      initSedona(sqlContext)
    }
  }
}
