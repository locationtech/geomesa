/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.locationtech.geomesa.spark.jts.encoders.SpatialEncoders

/**
 * User-facing module imports, sufficient for accessing the standard Spark-JTS functionality.
 */
package object jts extends DataFrameFunctions.Library with SpatialEncoders {
  /**
   * Initialization function that must be called before any JTS functionality
   * is accessed. This function can be called directly, or one of the `initJTS`
   * enrichment methods on [[SQLContext]] or [[SparkSession]] can be used instead.
   */
  def initJTS(sqlContext: SQLContext): Unit = {
    org.apache.spark.sql.jts.registerTypes()
    udf.registerFunctions(sqlContext)
    rules.registerOptimizations(sqlContext)
  }

  /** Enrichment over [[SQLContext]] to add `withJTS` "literate" method. */
  implicit class SQLContextWithJTS(val sqlContext: SQLContext) extends AnyVal {
    def withJTS: SQLContext = {
      initJTS(sqlContext)
      sqlContext
    }
  }

  /** Enrichment over [[SparkSession]] to add `withJTS` "literate" method. */
  implicit class SparkSessionWithJTS(val spark: SparkSession) extends AnyVal {
    def withJTS: SparkSession = {
      initJTS(spark.sqlContext)
      spark
    }
  }
}
