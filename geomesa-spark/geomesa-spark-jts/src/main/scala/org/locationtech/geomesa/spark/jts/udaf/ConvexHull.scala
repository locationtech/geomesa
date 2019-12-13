/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.jts.udaf

import org.locationtech.jts.geom.Geometry
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.Row
import org.apache.spark.sql.jts.JTSTypes

class ConvexHull extends UserDefinedAggregateFunction {
  import org.apache.spark.sql.types.{DataTypes => DT}

  override val inputSchema = DT.createStructType(Array(DT.createStructField("inputGeom", JTSTypes.GeometryTypeInstance, true)))
  override val bufferSchema = DT.createStructType(Array(DT.createStructField("convexHull", JTSTypes.GeometryTypeInstance, true)))
  override val dataType = DT.createStructType(Array(DT.createStructField("convexHull", JTSTypes.GeometryTypeInstance, true)))
  override val deterministic = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, null)
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val start = buffer.get(0)
    val geom = input.get(0).asInstanceOf[Geometry]
    if (start == null) {
      buffer.update(0, geom)
    } else {
      val ch = start.asInstanceOf[Geometry].union(geom).convexHull()
      buffer.update(0, ch)
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val ch =
      (buffer1.isNullAt(0), buffer2.isNullAt(0)) match {
        case (true, true)     => Option.empty[Geometry]
        case (false, false)   => Some(buffer1.getAs[Geometry](0).union(buffer2.getAs[Geometry](0)).convexHull())
        case (false, true)    => Some(buffer1.getAs[Geometry](0).convexHull())
        case (true, false)    => Some(buffer2.getAs[Geometry](0).convexHull())
      }
    ch.foreach { g => buffer1.update(0, g) }
  }

  override def evaluate(buffer: Row): Any = buffer
}

