/***********************************************************************
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
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
