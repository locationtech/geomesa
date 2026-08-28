/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.parquet.schema

import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.{Type, Types}
import org.locationtech.geomesa.utils.geotools.ObjectType
import org.locationtech.geomesa.utils.geotools.ObjectType.ObjectType

object GeometrySchema {

  val GeometryColumnX = "x"
  val GeometryColumnY = "y"

  /**
   * Enumeration of supported geometry encodings
   */
  sealed trait GeometryEncoding {

    /**
     * Create a builder for a parquet geometry field
     *
     * @param binding geometry type
     * @return
     */
    def schema(binding: ObjectType): Types.Builder[_, _ <: Type]
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
      override def schema(binding: ObjectType): Types.Builder[_, _ <: Type] = {
        binding match {
          case ObjectType.POINT =>
            Types.optionalGroup()
              .required(PrimitiveTypeName.DOUBLE).named(GeometryColumnX)
              .required(PrimitiveTypeName.DOUBLE).named(GeometryColumnY)

          case ObjectType.LINESTRING =>
            // TODO do we need multiple levels of nesting field ids???
            Types.optionalList()
              .requiredGroupElement() // the coordinate
              .required(PrimitiveTypeName.DOUBLE).named(GeometryColumnX)
              .required(PrimitiveTypeName.DOUBLE).named(GeometryColumnY)

          case ObjectType.MULTIPOINT =>
            Types.optionalList
              .requiredGroupElement() // the coordinate
              .required(PrimitiveTypeName.DOUBLE).named(GeometryColumnX)
              .required(PrimitiveTypeName.DOUBLE).named(GeometryColumnY)

          case ObjectType.POLYGON =>
            Types.optionalList
              .requiredListElement() // the coordinates of one ring
              .requiredGroupElement() // the coordinate
              .required(PrimitiveTypeName.DOUBLE).named(GeometryColumnX)
              .required(PrimitiveTypeName.DOUBLE).named(GeometryColumnY)

          case ObjectType.MULTILINESTRING =>
            Types.optionalList
              .requiredListElement() // the coordinates of one line string
              .requiredGroupElement() // the coordinate
              .required(PrimitiveTypeName.DOUBLE).named(GeometryColumnX)
              .required(PrimitiveTypeName.DOUBLE).named(GeometryColumnY)

          case ObjectType.MULTIPOLYGON =>
            Types.optionalList
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
    }

    case object GeoParquetWkb extends GeometryEncoding {
      override def schema(binding: ObjectType): Types.Builder[_, _ <: Type] =
        Types.primitive(PrimitiveTypeName.BINARY, Repetition.OPTIONAL)
    }
  }
}
