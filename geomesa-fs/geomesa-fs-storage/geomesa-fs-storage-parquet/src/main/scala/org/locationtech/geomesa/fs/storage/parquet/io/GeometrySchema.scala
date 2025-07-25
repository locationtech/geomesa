/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.parquet.io

import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.{GroupType, Type, Types}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.features.serialization.TwkbSerialization.GeometryBytes
import org.locationtech.geomesa.utils.geotools.ObjectType
import org.locationtech.geomesa.utils.geotools.ObjectType.ObjectType
import org.locationtech.geomesa.utils.text.StringSerialization.alphaNumericSafeString
import org.locationtech.jts.geom.{Geometry, Point}

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
  def apply(binding: ObjectType, encoding: GeometryEncoding): Types.Builder[_, _ <: Type] = {
    encoding.fn.lift(binding).getOrElse {
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

  object BoundingBoxField {

    import scala.collection.JavaConverters._

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
     * Gets the fields of this schema that have per-row bounding-boxes. We only add bounding boxes when
     * they help with predicate push-down, e.g. when a geometry is a non-point or WKB encoded
     *
     * @param sft simple feature type
     * @param encoding geometry encoding
     * @return
     */
    def apply(sft: SimpleFeatureType, encoding: GeometryEncoding): Seq[BoundingBoxField] = {
      sft.getAttributeDescriptors.asScala.toSeq.flatMap { d =>
        val binding = d.getType.getBinding
        if (classOf[Geometry].isAssignableFrom(binding) &&
            (encoding == GeometryEncoding.GeoParquetWkb || binding != classOf[Point])) {
          Some(BoundingBoxField(d.getLocalName))
        } else {
          None
        }
      }
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
    def schema(bbox: String): GroupType = {
      Types.optionalGroup()
        .required(PrimitiveTypeName.FLOAT).named(BoundingBoxField.XMin)
        .required(PrimitiveTypeName.FLOAT).named(BoundingBoxField.YMin)
        .required(PrimitiveTypeName.FLOAT).named(BoundingBoxField.XMax)
        .required(PrimitiveTypeName.FLOAT).named(BoundingBoxField.YMax)
        .named(bbox)
    }
  }

  /**
   * Enumeration of supported geometry encodings
   */
  sealed trait GeometryEncoding {
    protected[GeometrySchema] def fn: PartialFunction[ObjectType, _ <: Types.Builder[_, _ <: Type]]
  }

  object GeometryEncoding {

    def apply(name: String): GeometryEncoding = {
      Seq(GeoMesaV1, GeoMesaV0, GeoParquetNative, GeoParquetWkb).find(_.toString.equalsIgnoreCase(name)).getOrElse {
        throw new IllegalArgumentException(s"No geometry encoding defined for $name")
      }
    }

    case object GeoMesaV1 extends GeometryEncoding {
      override protected[GeometrySchema] val fn: PartialFunction[ObjectType, _ <: Types.Builder[_, _ <: Type]] = {
        case ObjectType.POINT =>
          Types.optionalGroup().id(GeometryBytes.TwkbPoint)
            .required(PrimitiveTypeName.DOUBLE).named(GeometryColumnX)
            .required(PrimitiveTypeName.DOUBLE).named(GeometryColumnY)

        case ObjectType.LINESTRING =>
          Types.optionalGroup().id(GeometryBytes.TwkbLineString)
            .repeated(PrimitiveTypeName.DOUBLE).named(GeometryColumnX)
            .repeated(PrimitiveTypeName.DOUBLE).named(GeometryColumnY)

        case ObjectType.MULTIPOINT =>
          Types.optionalGroup().id(GeometryBytes.TwkbMultiPoint)
            .repeated(PrimitiveTypeName.DOUBLE).named(GeometryColumnX)
            .repeated(PrimitiveTypeName.DOUBLE).named(GeometryColumnY)

        case ObjectType.POLYGON =>
          Types.optionalGroup().id(GeometryBytes.TwkbPolygon)
            .requiredList().element(PrimitiveTypeName.DOUBLE, Repetition.REPEATED).named(GeometryColumnX)
            .requiredList().element(PrimitiveTypeName.DOUBLE, Repetition.REPEATED).named(GeometryColumnY)

        case ObjectType.MULTILINESTRING =>
          Types.optionalGroup().id(GeometryBytes.TwkbMultiLineString)
            .requiredList().element(PrimitiveTypeName.DOUBLE, Repetition.REPEATED).named(GeometryColumnX)
            .requiredList().element(PrimitiveTypeName.DOUBLE, Repetition.REPEATED).named(GeometryColumnY)

        case ObjectType.MULTIPOLYGON =>
          Types.optionalGroup().id(GeometryBytes.TwkbMultiPolygon)
            .requiredList().requiredListElement().element(PrimitiveTypeName.DOUBLE, Repetition.REPEATED).named(GeometryColumnX)
            .requiredList().requiredListElement().element(PrimitiveTypeName.DOUBLE, Repetition.REPEATED).named(GeometryColumnY)

        case ObjectType.GEOMETRY_COLLECTION =>
          Types.primitive(PrimitiveTypeName.BINARY, Repetition.OPTIONAL)

        case ObjectType.GEOMETRY =>
          Types.primitive(PrimitiveTypeName.BINARY, Repetition.OPTIONAL)
      }
    }

    case object GeoMesaV0 extends GeometryEncoding {
      override protected[GeometrySchema] val fn: PartialFunction[ObjectType, _ <: Types.Builder[_, _ <: Type]] = {
        case ObjectType.POINT =>
          Types.buildGroup(Repetition.REQUIRED)
            .primitive(PrimitiveTypeName.DOUBLE, Repetition.REQUIRED).named(GeometryColumnX)
            .primitive(PrimitiveTypeName.DOUBLE, Repetition.REQUIRED).named(GeometryColumnY)
      }
    }

    case object GeoParquetNative extends GeometryEncoding {
      override protected[GeometrySchema] val fn: PartialFunction[ObjectType, _ <: Types.Builder[_, _ <: Type]] = {
        case ObjectType.POINT =>
          Types.optionalGroup().id(GeometryBytes.TwkbPoint)
            .required(PrimitiveTypeName.DOUBLE).named(GeometryColumnX)
            .required(PrimitiveTypeName.DOUBLE).named(GeometryColumnY)

        case ObjectType.LINESTRING =>
          Types.optionalList().id(GeometryBytes.TwkbLineString)
            .requiredGroupElement() // the coordinate
            .required(PrimitiveTypeName.DOUBLE).named(GeometryColumnX)
            .required(PrimitiveTypeName.DOUBLE).named(GeometryColumnY)

        case ObjectType.MULTIPOINT =>
          Types.optionalList.id(GeometryBytes.TwkbMultiPoint)
            .requiredGroupElement() // the coordinate
            .required(PrimitiveTypeName.DOUBLE).named(GeometryColumnX)
            .required(PrimitiveTypeName.DOUBLE).named(GeometryColumnY)

        case ObjectType.POLYGON =>
          Types.optionalList.id(GeometryBytes.TwkbPolygon)
            .requiredListElement() // the coordinates of one ring
            .requiredGroupElement() // the coordinate
            .required(PrimitiveTypeName.DOUBLE).named(GeometryColumnX)
            .required(PrimitiveTypeName.DOUBLE).named(GeometryColumnY)

        case ObjectType.MULTILINESTRING =>
          Types.optionalList.id(GeometryBytes.TwkbMultiLineString)
            .requiredListElement() // the coordinates of one line string
            .requiredGroupElement() // the coordinate
            .required(PrimitiveTypeName.DOUBLE).named(GeometryColumnX)
            .required(PrimitiveTypeName.DOUBLE).named(GeometryColumnY)

        case ObjectType.MULTIPOLYGON =>
          Types.optionalList.id(GeometryBytes.TwkbMultiPolygon)
            .requiredListElement() // the rings of the MultiPolygon
            .requiredListElement() // the coordinates of one ring
            .requiredGroupElement() // the coordinate
            .required(PrimitiveTypeName.DOUBLE).named(GeometryColumnX)
            .required(PrimitiveTypeName.DOUBLE).named(GeometryColumnY)

        case ObjectType.GEOMETRY_COLLECTION =>
          Types.primitive(PrimitiveTypeName.BINARY, Repetition.OPTIONAL)

        case ObjectType.GEOMETRY =>
          Types.primitive(PrimitiveTypeName.BINARY, Repetition.OPTIONAL)
      }
    }

    case object GeoParquetWkb extends GeometryEncoding {
      override protected[GeometrySchema] val fn: PartialFunction[ObjectType, _ <: Types.Builder[_, _ <: Type]] = {
        case _ => Types.primitive(PrimitiveTypeName.BINARY, Repetition.OPTIONAL)
      }
    }
  }
}
