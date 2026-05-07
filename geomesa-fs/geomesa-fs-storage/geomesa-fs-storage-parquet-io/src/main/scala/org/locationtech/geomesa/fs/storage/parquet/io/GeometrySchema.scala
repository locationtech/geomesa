/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.parquet.io

import org.apache.iceberg.types.Types.{FloatType, NestedField, StructType}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.{GroupType, Type, Types}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.utils.geotools.ObjectType
import org.locationtech.geomesa.utils.geotools.ObjectType.ObjectType
import org.locationtech.geomesa.utils.text.StringSerialization.alphaNumericSafeString
import org.locationtech.jts.geom.{Geometry, Point}

import java.util.concurrent.atomic.AtomicInteger

object GeometrySchema {

  val GeometryColumnX = "x"
  val GeometryColumnY = "y"

  /**
   * Holder for a bounding box field, along with a reference back to the original geometry field
   *
   * @param geometry name of the original geometry field being covered
   * @param bbox name of the bounding box field
   */
  case class BoundingBoxField(geometry: String, bbox: String)

  /**
   * Holder for multiple bounding box fields
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
  }

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
     * @param bbox name of a potential bbox field
     * @return
     */
    def fromBoundingBox(bbox: String): Option[BoundingBoxField] = {
      if (bbox.startsWith(BoundingBoxFieldPrefix) && bbox.endsWith(BoundingBoxFieldSuffix)) {
        Some(BoundingBoxField(bbox.substring(BoundingBoxFieldPrefix.length, bbox.length - BoundingBoxFieldSuffix.length), bbox))
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

  /**
   * Enumeration of supported geometry encodings
   */
  sealed trait GeometryEncoding {

    /**
     * Create a builder for a parquet geometry field
     *
     * @param binding geometry type
     * @param fieldIds field id tracker
     * @param nestedFieldIds nested field id tracker
     * @return
     */
    def schema(binding: ObjectType, fieldIds: AtomicInteger, nestedFieldIds: AtomicInteger): Types.Builder[_, _ <: Type]
  }

  object GeometryEncoding {

    def apply(name: String): GeometryEncoding = {
      Seq(GeoParquetNative, GeoParquetWkb).find(_.toString.equalsIgnoreCase(name)).getOrElse {
        if (name.equalsIgnoreCase("GeoMesaV0") || name.equalsIgnoreCase("GeoMesaV1")) {
          throw new UnsupportedOperationException(s"Geometry encoding '$name' is not longer supported")
        }
        throw new IllegalArgumentException(s"No geometry encoding defined for '$name'")
      }
    }

    case object GeoParquetNative extends GeometryEncoding {
      override def schema(binding: ObjectType, fieldIds: AtomicInteger, nestedFieldIds: AtomicInteger): Types.Builder[_, _ <: Type] = {
        binding match {
          case ObjectType.POINT =>
            Types.optionalGroup().id(fieldIds.getAndIncrement())
              .required(PrimitiveTypeName.DOUBLE).id(nestedFieldIds.getAndIncrement()).named(GeometryColumnX)
              .required(PrimitiveTypeName.DOUBLE).id(nestedFieldIds.getAndIncrement()).named(GeometryColumnY)

          case ObjectType.LINESTRING =>
            // TODO do we need multiple levels of nesting field ids???
            Types.optionalList().id(fieldIds.getAndIncrement())
              .requiredGroupElement().id(nestedFieldIds.getAndIncrement()) // the coordinate
              .required(PrimitiveTypeName.DOUBLE).id(nestedFieldIds.getAndIncrement()).named(GeometryColumnX)
              .required(PrimitiveTypeName.DOUBLE).id(nestedFieldIds.getAndIncrement()).named(GeometryColumnY)

          case ObjectType.MULTIPOINT =>
            Types.optionalList.id(fieldIds.getAndIncrement())
              .requiredGroupElement().id(nestedFieldIds.getAndIncrement()) // the coordinate
              .required(PrimitiveTypeName.DOUBLE).id(nestedFieldIds.getAndIncrement()).named(GeometryColumnX)
              .required(PrimitiveTypeName.DOUBLE).id(nestedFieldIds.getAndIncrement()).named(GeometryColumnY)

          case ObjectType.POLYGON =>
            Types.optionalList.id(fieldIds.getAndIncrement())
              .requiredListElement().id(nestedFieldIds.getAndIncrement()) // the coordinates of one ring
              .requiredGroupElement().id(nestedFieldIds.getAndIncrement()) // the coordinate
              .required(PrimitiveTypeName.DOUBLE).id(nestedFieldIds.getAndIncrement()).named(GeometryColumnX)
              .required(PrimitiveTypeName.DOUBLE).id(nestedFieldIds.getAndIncrement()).named(GeometryColumnY)

          case ObjectType.MULTILINESTRING =>
            Types.optionalList.id(fieldIds.getAndIncrement())
              .requiredListElement().id(nestedFieldIds.getAndIncrement()) // the coordinates of one line string
              .requiredGroupElement().id(nestedFieldIds.getAndIncrement()) // the coordinate
              .required(PrimitiveTypeName.DOUBLE).id(nestedFieldIds.getAndIncrement()).named(GeometryColumnX)
              .required(PrimitiveTypeName.DOUBLE).id(nestedFieldIds.getAndIncrement()).named(GeometryColumnY)

          case ObjectType.MULTIPOLYGON =>
            Types.optionalList.id(fieldIds.getAndIncrement())
              .requiredListElement().id(nestedFieldIds.getAndIncrement()) // the rings of the MultiPolygon
              .requiredListElement().id(nestedFieldIds.getAndIncrement()) // the coordinates of one ring
              .requiredGroupElement().id(nestedFieldIds.getAndIncrement()) // the coordinate
              .required(PrimitiveTypeName.DOUBLE).id(nestedFieldIds.getAndIncrement()).named(GeometryColumnX)
              .required(PrimitiveTypeName.DOUBLE).id(nestedFieldIds.getAndIncrement()).named(GeometryColumnY)

          case ObjectType.GEOMETRY_COLLECTION =>
            Types.primitive(PrimitiveTypeName.BINARY, Repetition.OPTIONAL).id(fieldIds.getAndIncrement())

          case ObjectType.GEOMETRY =>
            Types.primitive(PrimitiveTypeName.BINARY, Repetition.OPTIONAL).id(fieldIds.getAndIncrement())
        }
      }
    }

    case object GeoParquetWkb extends GeometryEncoding {
      override def schema(binding: ObjectType, fieldIds: AtomicInteger, nestedFieldIds: AtomicInteger): Types.Builder[_, _ <: Type] =
        Types.primitive(PrimitiveTypeName.BINARY, Repetition.OPTIONAL).id(fieldIds.getAndIncrement())
    }
  }
}
