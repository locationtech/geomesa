/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.parquet


package object io {

  import DateEncoding.DateEncoding
  import GeometrySchema.GeometryEncoding
  import ListEncoding.ListEncoding
  import UuidEncoding.UuidEncoding

  case object DateEncoding extends Enumeration {
    type DateEncoding = Value
    val Millis, Micros = Value
  }

  case object ListEncoding extends Enumeration {
    type ListEncoding = Value
    val ThreeLevel, TwoLevel = Value
  }

  case object UuidEncoding extends Enumeration {
    type UuidEncoding = Value
    val FixedLength, Bytes = Value
  }

  case class Encodings(geometry: GeometryEncoding, date: DateEncoding, list: ListEncoding, uuid: UuidEncoding)

  object Encodings {

    /**
     * For lists, dates, and uuids, we use geometry encoding as a proxy for version - if we're using an old geometry encoding,
     * then we use(d) the old encodings as well for back compatibility.
     *
     * @param geometries geometry encoding
     * @return
     */
    def apply(geometries: GeometryEncoding): Encodings = {
      if (geometries == GeometryEncoding.GeoMesaV0) {
        Encodings(geometries, DateEncoding.Millis, ListEncoding.TwoLevel, UuidEncoding.Bytes)
      } else if (geometries == GeometryEncoding.GeoMesaV1) {
        Encodings(geometries, DateEncoding.Millis, ListEncoding.TwoLevel, UuidEncoding.FixedLength)
      } else {
        Encodings(geometries, DateEncoding.Micros, ListEncoding.ThreeLevel, UuidEncoding.FixedLength)
      }
    }
  }
}
