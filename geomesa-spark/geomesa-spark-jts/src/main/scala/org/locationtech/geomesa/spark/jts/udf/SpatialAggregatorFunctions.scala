/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.jts.udf

import org.apache.spark.sql.SQLContext
import org.locationtech.geomesa.spark.jts.udaf.{Envelope_Aggr, Union_Aggr}

object SpatialAggregatorFunctions {
  private[geomesa] val ST_Union_Aggr = new Union_Aggr
  private[geomesa] val ST_Envelope_Aggr = new Envelope_Aggr

  private[geomesa] val aggregationNames = Map(
    ST_Union_Aggr -> "ST_Union_Aggr",
    ST_Envelope_Aggr -> "ST_Envelope_Aggr"
  )

  private[jts] def registerFunctions(sqlContext: SQLContext): Unit = {
    sqlContext.udf.register(aggregationNames(ST_Union_Aggr), ST_Union_Aggr)
    sqlContext.udf.register(aggregationNames(ST_Envelope_Aggr), ST_Envelope_Aggr)
  }
}
