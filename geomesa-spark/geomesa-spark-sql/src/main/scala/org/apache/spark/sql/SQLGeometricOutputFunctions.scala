/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.apache.spark.sql

import com.vividsolutions.jts.geom.Geometry
import org.locationtech.geomesa.spark.jts.util.SQLFunctionHelper.nullableUDF
import org.locationtech.geomesa.utils.geohash.GeoHash

object SQLGeometricOutputFunctions {
  val ST_GeoHash: (Geometry, Int) => String =
    nullableUDF((geom, prec) => GeoHash(geom.getInteriorPoint, prec).hash)

  def registerFunctions(sqlContext: SQLContext): Unit = {
    sqlContext.udf.register("st_geoHash", ST_GeoHash)
  }

}
