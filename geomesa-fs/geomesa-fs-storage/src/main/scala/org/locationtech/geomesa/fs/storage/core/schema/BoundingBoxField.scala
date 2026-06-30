/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.schema

import org.apache.iceberg.expressions.{Expression, Expressions}
import org.apache.iceberg.types.Types.{FloatType, NestedField, StructType}
import org.apache.parquet.filter2.predicate.{FilterApi, FilterPredicate}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.{GroupType, Types}

import java.util.concurrent.atomic.AtomicInteger

/**
 * Encoding for a bbox field
 */
object BoundingBoxField {

  val BoundingBoxFieldSuffix = s"_bbox${SimpleFeatureSchema.InternalFieldDelimiter}"

  val XMin = "xmin"
  val YMin = "ymin"
  val XMax = "xmax"
  val YMax = "ymax"

  /**
   * Gets the column name for the bbox group field
   *
   * @param geom geometry column name
   * @return
   */
  def groupName(geom: String): String = s"${SimpleFeatureSchema.InternalFieldDelimiter}$geom$BoundingBoxFieldSuffix"

  /**
   * The parquet schema for a bbox field
   *
   * @param geom geometry column name
   * @return
   */
  def parquetSchema(geom: String, fieldIds: AtomicInteger, nestedFieldIds: AtomicInteger): GroupType = {
    val bbox = groupName(geom)
    Types.optionalGroup()
      .id(fieldIds.getAndIncrement())
      .required(PrimitiveTypeName.FLOAT).id(nestedFieldIds.getAndIncrement()).named(BoundingBoxField.XMin)
      .required(PrimitiveTypeName.FLOAT).id(nestedFieldIds.getAndIncrement()).named(BoundingBoxField.YMin)
      .required(PrimitiveTypeName.FLOAT).id(nestedFieldIds.getAndIncrement()).named(BoundingBoxField.XMax)
      .required(PrimitiveTypeName.FLOAT).id(nestedFieldIds.getAndIncrement()).named(BoundingBoxField.YMax)
      .named(bbox)
  }

  /**
   * The iceberg schema for a bbox field
   *
   * @param geom geometry column name
   * @return
   */
  def icebergSchema(geom: String, fieldIds: AtomicInteger, nestedFieldIds: AtomicInteger): NestedField = {
    val bbox = groupName(geom)
    NestedField.optional(bbox).withId(fieldIds.getAndIncrement()).ofType(StructType.of(
      NestedField.required(BoundingBoxField.XMin).withId(nestedFieldIds.getAndIncrement()).ofType(FloatType.get()).build(),
      NestedField.required(BoundingBoxField.YMin).withId(nestedFieldIds.getAndIncrement()).ofType(FloatType.get()).build(),
      NestedField.required(BoundingBoxField.XMax).withId(nestedFieldIds.getAndIncrement()).ofType(FloatType.get()).build(),
      NestedField.required(BoundingBoxField.YMax).withId(nestedFieldIds.getAndIncrement()).ofType(FloatType.get()).build(),
    )).build()
  }

  /**
   * Create a filter against a bbox col
   *
   * @param geom geometry column name
   * @param xmin xmin
   * @param ymin ymin
   * @param xmax xmax
   * @param ymax ymax
   * @return
   */
  def filterParquet(geom: String, xmin: Double, ymin: Double, xmax: Double, ymax: Double): FilterPredicate = {
    val bbox = groupName(geom)
    val xminCol = FilterApi.floatColumn(s"$bbox.$XMin")
    val yminCol = FilterApi.floatColumn(s"$bbox.$YMin")
    val xmaxCol = FilterApi.floatColumn(s"$bbox.$XMax")
    val ymaxCol = FilterApi.floatColumn(s"$bbox.$YMax")
    val filters = Seq[FilterPredicate](
      FilterApi.ltEq(xminCol, Float.box(xmax.toFloat)),
      FilterApi.gtEq(xmaxCol, Float.box(xmin.toFloat)),
      FilterApi.ltEq(yminCol, Float.box(ymax.toFloat)),
      FilterApi.gtEq(ymaxCol, Float.box(ymin.toFloat))
    )
    filters.reduce(FilterApi.and)
  }

  /**
   * Create a filter against a bbox col
   *
   * @param geom geometry column name
   * @param xmin xmin
   * @param ymin ymin
   * @param xmax xmax
   * @param ymax ymax
   * @return
   */
  def filterIceberg(geom: String, xmin: Double, ymin: Double, xmax: Double, ymax: Double): Expression = {
    val bbox = groupName(geom)
    val exps = Seq(
      Expressions.lessThanOrEqual(s"$bbox.$XMin", Float.box(xmax.toFloat)),
      Expressions.greaterThanOrEqual(s"$bbox.$XMax", Float.box(xmin.toFloat)),
      Expressions.lessThanOrEqual(s"$bbox.$YMin", Float.box(ymax.toFloat)),
      Expressions.greaterThanOrEqual(s"$bbox.$YMax", Float.box(ymin.toFloat))
    )
    exps.reduce(Expressions.and)
  }
}
