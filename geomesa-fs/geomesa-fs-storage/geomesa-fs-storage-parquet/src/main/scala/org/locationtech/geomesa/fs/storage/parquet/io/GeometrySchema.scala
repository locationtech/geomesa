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
import org.apache.parquet.schema.{Type, Types}
import org.locationtech.geomesa.features.serialization.TwkbSerialization.GeometryBytes
import org.locationtech.geomesa.fs.storage.parquet.io.SimpleFeatureParquetSchema.{GeometryColumnX, GeometryColumnY}
import org.locationtech.geomesa.utils.geotools.ObjectType
import org.locationtech.geomesa.utils.geotools.ObjectType.ObjectType

object GeometrySchema {

  /**
   * Create a builder for a parquet geometry field
   *
   * @param binding geometry type
   * @param encoding encoding scheme
   * @return
   */
  def apply(binding: ObjectType, encoding: GeometryEncoding): Types.Builder[_, _ <: Type] = {
    encoding.fn.lift(binding).getOrElse {
      throw new NotImplementedError(s"No mapping defined for geometry type $binding with encoding $encoding")
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
          Types.buildGroup(Repetition.OPTIONAL).id(GeometryBytes.TwkbPoint)
            .required(PrimitiveTypeName.DOUBLE).named(GeometryColumnX)
            .required(PrimitiveTypeName.DOUBLE).named(GeometryColumnY)

        case ObjectType.LINESTRING =>
          Types.buildGroup(Repetition.OPTIONAL).id(GeometryBytes.TwkbLineString)
            .repeated(PrimitiveTypeName.DOUBLE).named(GeometryColumnX)
            .repeated(PrimitiveTypeName.DOUBLE).named(GeometryColumnY)

        case ObjectType.MULTIPOINT =>
          Types.buildGroup(Repetition.OPTIONAL).id(GeometryBytes.TwkbMultiPoint)
            .repeated(PrimitiveTypeName.DOUBLE).named(GeometryColumnX)
            .repeated(PrimitiveTypeName.DOUBLE).named(GeometryColumnY)

        case ObjectType.POLYGON =>
          Types.buildGroup(Repetition.OPTIONAL).id(GeometryBytes.TwkbPolygon)
            .requiredList().element(PrimitiveTypeName.DOUBLE, Repetition.REPEATED).named(GeometryColumnX)
            .requiredList().element(PrimitiveTypeName.DOUBLE, Repetition.REPEATED).named(GeometryColumnY)

        case ObjectType.MULTILINESTRING =>
          Types.buildGroup(Repetition.OPTIONAL).id(GeometryBytes.TwkbMultiLineString)
            .requiredList().element(PrimitiveTypeName.DOUBLE, Repetition.REPEATED).named(GeometryColumnX)
            .requiredList().element(PrimitiveTypeName.DOUBLE, Repetition.REPEATED).named(GeometryColumnY)

        case ObjectType.MULTIPOLYGON =>
          Types.buildGroup(Repetition.OPTIONAL).id(GeometryBytes.TwkbMultiPolygon)
            .requiredList().requiredListElement().element(PrimitiveTypeName.DOUBLE, Repetition.REPEATED).named(GeometryColumnX)
            .requiredList().requiredListElement().element(PrimitiveTypeName.DOUBLE, Repetition.REPEATED).named(GeometryColumnY)

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
          Types.buildGroup(Repetition.OPTIONAL).id(GeometryBytes.TwkbPoint)
            .required(PrimitiveTypeName.DOUBLE).named(GeometryColumnX)
            .required(PrimitiveTypeName.DOUBLE).named(GeometryColumnY)

        case ObjectType.LINESTRING =>
          Types.list(Repetition.OPTIONAL).id(GeometryBytes.TwkbLineString)
            .requiredGroupElement() // the coordinate
            .required(PrimitiveTypeName.DOUBLE).named(GeometryColumnX)
            .required(PrimitiveTypeName.DOUBLE).named(GeometryColumnY)

        case ObjectType.MULTIPOINT =>
          Types.list(Repetition.OPTIONAL).id(GeometryBytes.TwkbMultiPoint)
            .requiredGroupElement() // the coordinate
            .required(PrimitiveTypeName.DOUBLE).named(GeometryColumnX)
            .required(PrimitiveTypeName.DOUBLE).named(GeometryColumnY)

        case ObjectType.POLYGON =>
          Types.list(Repetition.OPTIONAL).id(GeometryBytes.TwkbPolygon)
            .requiredListElement() // the coordinates of one ring
            .requiredGroupElement() // the coordinate
            .required(PrimitiveTypeName.DOUBLE).named(GeometryColumnX)
            .required(PrimitiveTypeName.DOUBLE).named(GeometryColumnY)

        case ObjectType.MULTILINESTRING =>
          Types.list(Repetition.OPTIONAL).id(GeometryBytes.TwkbMultiLineString)
            .requiredListElement() // the coordinates of one line string
            .requiredGroupElement() // the coordinate
            .required(PrimitiveTypeName.DOUBLE).named(GeometryColumnX)
            .required(PrimitiveTypeName.DOUBLE).named(GeometryColumnY)

        case ObjectType.MULTIPOLYGON =>
          Types.list(Repetition.OPTIONAL).id(GeometryBytes.TwkbMultiPolygon)
            .requiredListElement() // the rings of the MultiPolygon
            .requiredListElement() // the coordinates of one ring
            .requiredGroupElement() // the coordinate
            .required(PrimitiveTypeName.DOUBLE).named(GeometryColumnX)
            .required(PrimitiveTypeName.DOUBLE).named(GeometryColumnY)

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
