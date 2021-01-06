/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.jts

import org.apache.spark.sql.SQLContext

package object rules {
  /** Registration delegation function. New rule sets should be added here. */
  def registerOptimizations(sqlContext: SQLContext): Unit = {
    GeometryLiteralRules.registerOptimizations(sqlContext)
  }
}
