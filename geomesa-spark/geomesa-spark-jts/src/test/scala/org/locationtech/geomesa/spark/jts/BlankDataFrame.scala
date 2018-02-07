
/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.jts

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * Mixin providing constructor for creating a DataFrame with a single row and no columns.
 * Useful for testing the invocation of data constructing UDFs.
 */
trait BlankDataFrame {
  def dfBlank(implicit spark: SparkSession): DataFrame = {
    // This is to enable us to do a single row creation select operation in DataFrame
    // world. Probably a better/easier way of doing this.
    spark.createDataFrame(spark.sparkContext.makeRDD(Seq(Row())), StructType(Seq.empty))
  }
}
