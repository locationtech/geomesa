/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.schema

import org.apache.iceberg.types.Types.{NestedField, StringType}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.{LogicalTypeAnnotation, PrimitiveType, Types}
import org.locationtech.geomesa.utils.geotools.ObjectType
import org.locationtech.geomesa.utils.geotools.ObjectType.ObjectType

import java.util.concurrent.atomic.AtomicInteger

object ZValueField {

  val ZValueFieldPrefix = "__"
  val Z2ValueFieldSuffix = "_z2__"
  val XZ2ValueFieldSuffix = "_xz2__"

  /**
   * Gets the column name for the bbox group field
   *
   * @param geom geometry column name
   * @return
   */
  def xz2FieldName(geom: String): String = s"$ZValueFieldPrefix$geom$XZ2ValueFieldSuffix"

  /**
   * Gets the column name for the bbox group field
   *
   * @param geom geometry column name
   * @return
   */
  def z2FieldName(geom: String): String = s"$ZValueFieldPrefix$geom$Z2ValueFieldSuffix"

  /**
   * The parquet schema for a z-value field
   *
   * @param geom geometry column name
   * @param geometryType geom type
   * @return
   */
  def parquetSchema(geom: String, geometryType: ObjectType, fieldIds: AtomicInteger): PrimitiveType =
    Types.optional(PrimitiveTypeName.BINARY).id(fieldIds.getAndIncrement()).as(LogicalTypeAnnotation.stringType()).named(name(geom, geometryType))

  /**
   * The iceberg schema for a z-value field
   *
   * @param geom geometry column name
   * @param geometryType geom type
   * @return
   */
  def icebergSchema(geom: String, geometryType: ObjectType, fieldIds: AtomicInteger): NestedField =
    NestedField.optional(name(geom, geometryType)).withId(fieldIds.getAndIncrement()).ofType(StringType.get()).build()

  private def name(geom: String, geometryType: ObjectType): String = geometryType match {
    case ObjectType.POINT => z2FieldName(geom)
    case _ => xz2FieldName(geom)
  }
}
