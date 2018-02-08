package org.locationtech.geomesa.spark.jts

import org.apache.spark.sql.SQLContext

/**
 *
 * @author sfitch 
 * @since 2/7/18
 */
package object udf {
  def registerFunctions(sqlContext: SQLContext): Unit = {
    GeometricAccessorFunctions.registerFunctions(sqlContext)
    GeometricCastFunctions.registerFunctions(sqlContext)
    GeometricConstructorFunctions.registerFunctions(sqlContext)
    GeometricOutputFunctions.registerFunctions(sqlContext)
    SpatialRelationFunctions.registerFunctions(sqlContext)
  }
}
