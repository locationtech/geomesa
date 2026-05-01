/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.parquet.io

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
   * Create a builder for a parquet geometry field
   *
   * @param binding geometry type
   * @param encoding encoding scheme
   * @return
   */
  def apply(binding: ObjectType, encoding: GeometryEncoding, fieldIds: AtomicInteger): Types.Builder[_, _ <: Type] = {
    encoding.fn.lift(binding).map(_.apply(fieldIds)).getOrElse {
      throw new UnsupportedOperationException(s"No mapping defined for geometry type $binding with encoding $encoding")
    }
  }

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
    def schema(bbox: String, fieldIds: AtomicInteger): GroupType = {
      Types.optionalGroup()
        .id(fieldIds.getAndIncrement())
        .required(PrimitiveTypeName.FLOAT).id(fieldIds.getAndIncrement()).named(BoundingBoxField.XMin)
        .required(PrimitiveTypeName.FLOAT).id(fieldIds.getAndIncrement()).named(BoundingBoxField.YMin)
        .required(PrimitiveTypeName.FLOAT).id(fieldIds.getAndIncrement()).named(BoundingBoxField.XMax)
        .required(PrimitiveTypeName.FLOAT).id(fieldIds.getAndIncrement()).named(BoundingBoxField.YMax)
        .named(bbox)
    }
  }

  /**
   * Enumeration of supported geometry encodings
   */
  sealed trait GeometryEncoding {
    protected[GeometrySchema] def fn: PartialFunction[ObjectType, AtomicInteger => _ <: Types.Builder[_, _ <: Type]]
  }

  object GeometryEncoding {

    def apply(name: String): GeometryEncoding = {
      Seq(GeoMesaV1, GeoParquetNative, GeoParquetWkb).find(_.toString.equalsIgnoreCase(name)).getOrElse {
        throw new IllegalArgumentException(s"No geometry encoding defined for $name")
      }
    }

    case object GeoMesaV1 extends GeometryEncoding {
      override protected[GeometrySchema] val fn: PartialFunction[ObjectType, AtomicInteger => _ <: Types.Builder[_, _ <: Type]] = {
        case ObjectType.POINT =>
          ids: AtomicInteger =>
            Types.optionalGroup().id(ids.getAndIncrement)
              .required(PrimitiveTypeName.DOUBLE).id(ids.getAndIncrement).named(GeometryColumnX)
              .required(PrimitiveTypeName.DOUBLE).id(ids.getAndIncrement).named(GeometryColumnY)

        case ObjectType.LINESTRING =>
          ids: AtomicInteger =>
            Types.optionalGroup().id(ids.getAndIncrement())
              .repeated(PrimitiveTypeName.DOUBLE).id(ids.getAndIncrement()).named(GeometryColumnX)
              .repeated(PrimitiveTypeName.DOUBLE).id(ids.getAndIncrement()).named(GeometryColumnY)

        case ObjectType.MULTIPOINT =>
          ids: AtomicInteger =>
            Types.optionalGroup().id(ids.getAndIncrement())
              .repeated(PrimitiveTypeName.DOUBLE).id(ids.getAndIncrement()).named(GeometryColumnX)
              .repeated(PrimitiveTypeName.DOUBLE).id(ids.getAndIncrement()).named(GeometryColumnY)

        case ObjectType.POLYGON =>
          ids: AtomicInteger =>
            Types.optionalGroup().id(ids.getAndIncrement())
              .requiredList().id(ids.getAndIncrement()).element(PrimitiveTypeName.DOUBLE, Repetition.REPEATED).id(ids.getAndIncrement()).named(GeometryColumnX)
              .requiredList().id(ids.getAndIncrement()).element(PrimitiveTypeName.DOUBLE, Repetition.REPEATED).id(ids.getAndIncrement()).named(GeometryColumnY)

        case ObjectType.MULTILINESTRING =>
          ids: AtomicInteger =>
            Types.optionalGroup().id(ids.getAndIncrement())
              .requiredList().id(ids.getAndIncrement()).element(PrimitiveTypeName.DOUBLE, Repetition.REPEATED).id(ids.getAndIncrement()).named(GeometryColumnX)
              .requiredList().id(ids.getAndIncrement()).element(PrimitiveTypeName.DOUBLE, Repetition.REPEATED).id(ids.getAndIncrement()).named(GeometryColumnY)

        case ObjectType.MULTIPOLYGON =>
          ids: AtomicInteger =>
            Types.optionalGroup().id(ids.getAndIncrement())
              .requiredList().id(ids.getAndIncrement()).requiredListElement().id(ids.getAndIncrement()).element(PrimitiveTypeName.DOUBLE, Repetition.REPEATED).id(ids.getAndIncrement()).named(GeometryColumnX)
              .requiredList().id(ids.getAndIncrement()).requiredListElement().id(ids.getAndIncrement()).element(PrimitiveTypeName.DOUBLE, Repetition.REPEATED).id(ids.getAndIncrement()).named(GeometryColumnY)

        case ObjectType.GEOMETRY_COLLECTION =>
          ids: AtomicInteger =>
            Types.primitive(PrimitiveTypeName.BINARY, Repetition.OPTIONAL).id(ids.getAndIncrement())

        case ObjectType.GEOMETRY =>
          ids: AtomicInteger =>
            Types.primitive(PrimitiveTypeName.BINARY, Repetition.OPTIONAL).id(ids.getAndIncrement())
      }
    }

    case object GeoParquetNative extends GeometryEncoding {
      override protected[GeometrySchema] val fn: PartialFunction[ObjectType, AtomicInteger => _ <: Types.Builder[_, _ <: Type]] = {
        case ObjectType.POINT =>
          ids: AtomicInteger =>
            Types.optionalGroup().id(ids.getAndIncrement())
              .required(PrimitiveTypeName.DOUBLE).id(ids.getAndIncrement()).named(GeometryColumnX)
              .required(PrimitiveTypeName.DOUBLE).id(ids.getAndIncrement()).named(GeometryColumnY)

        case ObjectType.LINESTRING =>
          ids: AtomicInteger =>
            Types.optionalList().id(ids.getAndIncrement())
              .requiredGroupElement().id(ids.getAndIncrement()) // the coordinate
              .required(PrimitiveTypeName.DOUBLE).id(ids.getAndIncrement()).named(GeometryColumnX)
              .required(PrimitiveTypeName.DOUBLE).id(ids.getAndIncrement()).named(GeometryColumnY)

        case ObjectType.MULTIPOINT =>
          ids: AtomicInteger =>
            Types.optionalList.id(ids.getAndIncrement())
              .requiredGroupElement().id(ids.getAndIncrement()) // the coordinate
              .required(PrimitiveTypeName.DOUBLE).id(ids.getAndIncrement()).named(GeometryColumnX)
              .required(PrimitiveTypeName.DOUBLE).id(ids.getAndIncrement()).named(GeometryColumnY)

        case ObjectType.POLYGON =>
          ids: AtomicInteger =>
            Types.optionalList.id(ids.getAndIncrement())
              .requiredListElement().id(ids.getAndIncrement()) // the coordinates of one ring
              .requiredGroupElement().id(ids.getAndIncrement()) // the coordinate
              .required(PrimitiveTypeName.DOUBLE).id(ids.getAndIncrement()).named(GeometryColumnX)
              .required(PrimitiveTypeName.DOUBLE).id(ids.getAndIncrement()).named(GeometryColumnY)

        case ObjectType.MULTILINESTRING =>
          ids: AtomicInteger =>
            Types.optionalList.id(ids.getAndIncrement())
              .requiredListElement().id(ids.getAndIncrement()) // the coordinates of one line string
              .requiredGroupElement().id(ids.getAndIncrement()) // the coordinate
              .required(PrimitiveTypeName.DOUBLE).id(ids.getAndIncrement()).named(GeometryColumnX)
              .required(PrimitiveTypeName.DOUBLE).id(ids.getAndIncrement()).named(GeometryColumnY)

        case ObjectType.MULTIPOLYGON =>
          ids: AtomicInteger =>
            Types.optionalList.id(ids.getAndIncrement())
              .requiredListElement().id(ids.getAndIncrement()) // the rings of the MultiPolygon
              .requiredListElement().id(ids.getAndIncrement()) // the coordinates of one ring
              .requiredGroupElement().id(ids.getAndIncrement()) // the coordinate
              .required(PrimitiveTypeName.DOUBLE).id(ids.getAndIncrement()).named(GeometryColumnX)
              .required(PrimitiveTypeName.DOUBLE).id(ids.getAndIncrement()).named(GeometryColumnY)

        case ObjectType.GEOMETRY_COLLECTION =>
          ids: AtomicInteger =>
            Types.primitive(PrimitiveTypeName.BINARY, Repetition.OPTIONAL).id(ids.getAndIncrement())

        case ObjectType.GEOMETRY =>
          ids: AtomicInteger =>
            Types.primitive(PrimitiveTypeName.BINARY, Repetition.OPTIONAL).id(ids.getAndIncrement())
      }
    }

    case object GeoParquetWkb extends GeometryEncoding {
      override protected[GeometrySchema] val fn: PartialFunction[ObjectType, AtomicInteger => _ <: Types.Builder[_, _ <: Type]] = {
        case _ =>
          ids: AtomicInteger => Types.primitive(PrimitiveTypeName.BINARY, Repetition.OPTIONAL).id(ids.getAndIncrement())
      }
    }
  }
}
