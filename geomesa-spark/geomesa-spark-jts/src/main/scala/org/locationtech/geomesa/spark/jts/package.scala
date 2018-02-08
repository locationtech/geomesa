package org.locationtech.geomesa.spark

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.locationtech.geomesa.spark.jts.encoders.SpatialEncoders

/**
 * User-facing module imports, sufficient for accessing the standard Spark-JTS functionality.
 */
package object jts extends DataFrameFunctions.Library with SpatialEncoders {
  def initJTS(sqlContext: SQLContext): Unit = {
    org.apache.spark.sql.jts.registerTypes()
    udf.registerFunctions(sqlContext)
    rules.registerOptimizations(sqlContext)
  }

  implicit class SQLContextWithJTS(sqlContext: SQLContext) {
    def withJTS: SQLContext = {
      initJTS(sqlContext)
      sqlContext
    }
  }

  implicit class SparkSessionWithJTS(spark: SparkSession) {
    def withJTS: SparkSession = {
      initJTS(spark.sqlContext)
      spark
    }
  }
}
