/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.parquet.io.geometry

import org.apache.iceberg.types.Types.{FloatType, NestedField, StructType}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.{GroupType, Types}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.fs.storage.parquet.io.geometry.BoundingBoxes.BoundingBoxField
import org.locationtech.geomesa.fs.storage.parquet.io.geometry.GeometrySchema.GeometryEncoding
import org.locationtech.geomesa.utils.text.StringSerialization.alphaNumericSafeString
import org.locationtech.jts.geom.{Geometry, Point}

import java.util.concurrent.atomic.AtomicInteger

/**
 * Mapping of fields to bounding box fields
 *
 * @param fields fields
 */
case class BoundingBoxes(fields: Seq[BoundingBoxField]) {
  private val fieldMap = fields.map(f => f.geometry -> f.bbox).toMap
  def isEmpty: Boolean = fields.isEmpty
  def nonEmpty: Boolean = fields.nonEmpty
  def get(field: String): Option[String] = fieldMap.get(field)
}

object BoundingBoxes {

  import scala.collection.JavaConverters._

  /**
   * Gets the fields of this schema that have per-row bounding-boxes. We only add bounding boxes when
   * they help with predicate push-down, e.g. when a geometry is a non-point or WKB encoded
   *
   * @param sft simple feature type
   * @param encoding geometry encoding
   * @return
   */
  def apply(sft: SimpleFeatureType, encoding: GeometryEncoding): BoundingBoxes = {
    val bboxes = sft.getAttributeDescriptors.asScala.toSeq.flatMap { d =>
      val binding = d.getType.getBinding
      if (classOf[Geometry].isAssignableFrom(binding) &&
        (encoding == GeometryEncoding.GeoParquetWkb || binding != classOf[Point])) {
        Some(BoundingBoxField(d.getLocalName))
      } else {
        None
      }
    }
    BoundingBoxes(bboxes)
  }

  /**
   * Holder for a bounding box field, along with a reference back to the original geometry field
   *
   * @param geometry name of the original geometry field being covered
   * @param bbox name of the bounding box field
   */
  case class BoundingBoxField(geometry: String, bbox: String)

  object BoundingBoxField {

    val BoundingBoxFieldPrefix = "__"
    val BoundingBoxFieldSuffix = "_bbox__"

    val XMin = "xmin"
    val YMin = "ymin"
    val XMax = "xmax"
    val YMax = "ymax"

    def apply(geometry: String, encoded: Boolean = false): BoundingBoxField = {
      val geom = if (encoded) { geometry } else { alphaNumericSafeString(geometry) }
      val bbox = s"$BoundingBoxFieldPrefix$geom$BoundingBoxFieldSuffix"
      BoundingBoxField(geom, bbox)
    }

    /**
     * Creates a field name based on a bounding box field
     *
     * @param field name of a potential bbox field
     * @return
     */
    def fromFieldName(field: String): Option[BoundingBoxField] = {
      if (field.startsWith(BoundingBoxFieldPrefix) && field.endsWith(BoundingBoxFieldSuffix)) {
        Some(BoundingBoxField(field.substring(BoundingBoxFieldPrefix.length, field.length - BoundingBoxFieldSuffix.length), field))
      } else {
        None
      }
    }

    /**
     * The parquet schema for a bbox field
     *
     * @param bbox field name
     * @return
     */
    def schema(bbox: String, fieldIds: AtomicInteger, nestedFieldIds: AtomicInteger): GroupType = {
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
     * @param bbox field name
     * @return
     */
    def icebergSchema(bbox: String, fieldIds: AtomicInteger, nestedFieldIds: AtomicInteger): NestedField = {
      NestedField.optional(bbox).withId(fieldIds.getAndIncrement()).ofType(StructType.of(
        NestedField.required(BoundingBoxField.XMin).withId(nestedFieldIds.getAndIncrement()).ofType(FloatType.get()).build(),
        NestedField.required(BoundingBoxField.YMin).withId(nestedFieldIds.getAndIncrement()).ofType(FloatType.get()).build(),
        NestedField.required(BoundingBoxField.XMax).withId(nestedFieldIds.getAndIncrement()).ofType(FloatType.get()).build(),
        NestedField.required(BoundingBoxField.YMax).withId(nestedFieldIds.getAndIncrement()).ofType(FloatType.get()).build(),
      )).build()
    }
  }
}
