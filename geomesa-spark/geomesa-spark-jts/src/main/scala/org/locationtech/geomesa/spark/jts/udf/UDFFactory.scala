/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.jts.udf

import org.apache.spark.sql.{Encoder, Encoders, SQLContext}
import org.locationtech.geomesa.spark.jts.encoders.{SparkDefaultEncoders, SpatialEncoders}
import org.locationtech.geomesa.spark.jts.udf.UDFFactory.Registerable

trait UDFFactory extends SparkDefaultEncoders with SpatialEncoders {
  implicit def integerEncoder: Encoder[Integer] = Encoders.INT
  def udfs: Seq[Registerable]
}

object UDFFactory {
  trait Registerable {
    def register(sqlContext: SQLContext): Unit
  }
}
