/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.parquet.io.geometry

import org.apache.iceberg.types.Types.{LongType, NestedField, StringType}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.{LogicalTypeAnnotation, PrimitiveType, Types}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.fs.storage.parquet.io.geometry.ZValues.ZValueField
import org.locationtech.geomesa.utils.text.StringSerialization.alphaNumericSafeString
import org.locationtech.jts.geom.{Geometry, Point}

import java.util.concurrent.atomic.AtomicInteger

/**
 * Mapping of fields to z-value fields
 *
 * @param fields fields
 */
case class ZValues(fields: Seq[ZValueField]) {
  private val fieldMap = fields.map(f => f.geometry -> f.zValue).toMap
  def isEmpty: Boolean = fields.isEmpty
  def nonEmpty: Boolean = fields.nonEmpty
  def get(field: String): Option[String] = fieldMap.get(field)
}

object ZValues {

  import scala.collection.JavaConverters._

  /**
   * Gets the fields of this schema that have per-row z-values
   *
   * @param sft simple feature type
   * @return
   */
  def apply(sft: SimpleFeatureType): ZValues = {
    val fields = sft.getAttributeDescriptors.asScala.toSeq.flatMap { d =>
      val binding = d.getType.getBinding
      if (binding == classOf[Point]) {
        Some(ZValueField.z2(d.getLocalName))
      } else if (classOf[Geometry].isAssignableFrom(binding)) {
        Some(ZValueField.xz2(d.getLocalName))
      } else {
        None
      }
    }
    ZValues(fields)
  }

  /**
   * Holder for a z-value field, along with a reference back to the original geometry field
   *
   * @param geometry name of the original geometry field being covered
   * @param zValue name of the z-value field
   */
  case class ZValueField(geometry: String, zValue: String)

  object ZValueField {

    val ZValueFieldPrefix = "__"
    val Z2ValueFieldSuffix = "_z2__"
    val XZ2ValueFieldSuffix = "_xz2__"

    def z2(geometry: String, encoded: Boolean = false): ZValueField = {
      val geom = if (encoded) { geometry } else { alphaNumericSafeString(geometry) }
      val zValue = s"$ZValueFieldPrefix$geom$Z2ValueFieldSuffix"
      ZValueField(geom, zValue)
    }

    def xz2(geometry: String, encoded: Boolean = false): ZValueField = {
      val geom = if (encoded) { geometry } else { alphaNumericSafeString(geometry) }
      val zValue = s"$ZValueFieldPrefix$geom$XZ2ValueFieldSuffix"
      ZValueField(geom, zValue)
    }

    /**
     * Creates a field name based on a z-value field
     *
     * @param field name of a potential z-value field
     * @return
     */
    def fromFieldName(field: String): Option[ZValueField] = {
      if (field.startsWith(ZValueFieldPrefix)) {
        Seq(Z2ValueFieldSuffix, XZ2ValueFieldSuffix).collectFirst {
          case suffix if field.endsWith(suffix) =>
            ZValueField(field.substring(ZValueFieldPrefix.length, field.length - suffix.length), field)
        }
      } else {
        None
      }
    }

    /**
     * The parquet schema for a z-value field
     *
     * @param zValue field name
     * @return
     */
    def schema(zValue: String, fieldIds: AtomicInteger): PrimitiveType =
      Types.optional(PrimitiveTypeName.BINARY).id(fieldIds.getAndIncrement()).as(LogicalTypeAnnotation.stringType()).named(zValue)

    /**
     * The iceberg schema for a z-value field
     *
     * @param zValue field name
     * @return
     */
    def icebergSchema(zValue: String, fieldIds: AtomicInteger): NestedField =
      NestedField.optional(zValue).withId(fieldIds.getAndIncrement()).ofType(StringType.get()).build()
  }
}
