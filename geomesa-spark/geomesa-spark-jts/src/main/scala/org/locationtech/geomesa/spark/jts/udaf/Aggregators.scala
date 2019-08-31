/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.jts.udaf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.jts.JTSTypes
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory, Polygon}


class Union_Aggr extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType(StructField("Union", JTSTypes.GeometryTypeInstance) :: Nil)

  override def bufferSchema: StructType = StructType(StructField("Union", JTSTypes.GeometryTypeInstance) :: Nil)

  override def dataType: DataType = JTSTypes.GeometryTypeInstance

  override def deterministic: Boolean = true

  private[geomesa] val geometryFactory = new GeometryFactory()

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    val coordinates: Array[Coordinate] = new Array[Coordinate](5)
    coordinates(0) = new Coordinate(-999999999, -999999999)
    coordinates(1) = new Coordinate(-999999999, -999999999)
    coordinates(2) = new Coordinate(-999999999, -999999999)
    coordinates(3) = new Coordinate(-999999999, -999999999)
    coordinates(4) = coordinates(0)
    buffer(0) = geometryFactory.createPolygon(coordinates)
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val accumulateUnion = buffer.getAs[Polygon](0)
    val newPolygon = input.getAs[Polygon](0)
    if (accumulateUnion.getArea == 0) buffer(0) = newPolygon
    else buffer(0) = accumulateUnion.union(newPolygon)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val leftPolygon = buffer1.getAs[Polygon](0)
    val rightPolygon = buffer2.getAs[Polygon](0)
    if (leftPolygon.getCoordinates()(0).x == -999999999) buffer1(0) = rightPolygon
    else if (rightPolygon.getCoordinates()(0).x == -999999999) buffer1(0) = leftPolygon
    else buffer1(0) = leftPolygon.union(rightPolygon)
  }

  override def evaluate(buffer: Row): Any = {
    return buffer.getAs[Geometry](0)
  }
}

class Envelope_Aggr extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType(StructField("Envelope", JTSTypes.GeometryTypeInstance) :: Nil)

  override def bufferSchema: StructType = StructType(StructField("Envelope", JTSTypes.GeometryTypeInstance) :: Nil)

  override def dataType: DataType = JTSTypes.GeometryTypeInstance

  override def deterministic: Boolean = true

  private[geomesa] val geometryFactory = new GeometryFactory()

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    val coordinates: Array[Coordinate] = new Array[Coordinate](5)
    coordinates(0) = new Coordinate(-999999999, -999999999)
    coordinates(1) = new Coordinate(-999999999, -999999999)
    coordinates(2) = new Coordinate(-999999999, -999999999)
    coordinates(3) = new Coordinate(-999999999, -999999999)
    coordinates(4) = new Coordinate(-999999999, -999999999)
    buffer(0) = geometryFactory.createPolygon(coordinates)
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val accumulateEnvelope = buffer.getAs[Geometry](0).getEnvelopeInternal
    val newEnvelope = input.getAs[Geometry](0).getEnvelopeInternal
    val coordinates: Array[Coordinate] = new Array[Coordinate](5)
    var minX = 0.0
    var minY = 0.0
    var maxX = 0.0
    var maxY = 0.0
    if (accumulateEnvelope.getMinX == -999999999) {
      minX = newEnvelope.getMinX
      minY = newEnvelope.getMinY
      maxX = newEnvelope.getMaxX
      maxY = newEnvelope.getMaxY
    }
    else {
      minX = Math.min(accumulateEnvelope.getMinX, newEnvelope.getMinX)
      minY = Math.min(accumulateEnvelope.getMinY, newEnvelope.getMinY)
      maxX = Math.max(accumulateEnvelope.getMaxX, newEnvelope.getMaxX)
      maxY = Math.max(accumulateEnvelope.getMaxY, newEnvelope.getMaxY)
    }
    coordinates(0) = new Coordinate(minX, minY)
    coordinates(1) = new Coordinate(minX, maxY)
    coordinates(2) = new Coordinate(maxX, maxY)
    coordinates(3) = new Coordinate(maxX, minY)
    coordinates(4) = coordinates(0)
    buffer(0) = geometryFactory.createPolygon(coordinates)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val leftEnvelope = buffer1.getAs[Geometry](0).getEnvelopeInternal
    val rightEnvelope = buffer2.getAs[Geometry](0).getEnvelopeInternal
    val coordinates: Array[Coordinate] = new Array[Coordinate](5)
    var minX = 0.0
    var minY = 0.0
    var maxX = 0.0
    var maxY = 0.0
    if (leftEnvelope.getMinX == -999999999) {
      // Found the leftEnvelope is the initial value
      minX = rightEnvelope.getMinX
      minY = rightEnvelope.getMinY
      maxX = rightEnvelope.getMaxX
      maxY = rightEnvelope.getMaxY
    }
    else if (rightEnvelope.getMinX == -999999999) {
      // Found the rightEnvelope is the initial value
      minX = leftEnvelope.getMinX
      minY = leftEnvelope.getMinY
      maxX = leftEnvelope.getMaxX
      maxY = leftEnvelope.getMaxY
    }
    else {
      minX = Math.min(leftEnvelope.getMinX, rightEnvelope.getMinX)
      minY = Math.min(leftEnvelope.getMinY, rightEnvelope.getMinY)
      maxX = Math.max(leftEnvelope.getMaxX, rightEnvelope.getMaxX)
      maxY = Math.max(leftEnvelope.getMaxY, rightEnvelope.getMaxY)
    }
    coordinates(0) = new Coordinate(minX, minY)
    coordinates(1) = new Coordinate(minX, maxY)
    coordinates(2) = new Coordinate(maxX, maxY)
    coordinates(3) = new Coordinate(maxX, minY)
    coordinates(4) = coordinates(0)
    buffer1(0) = geometryFactory.createPolygon(coordinates)
  }

  override def evaluate(buffer: Row): Any = {
    return buffer.getAs[Geometry](0)
  }
}
