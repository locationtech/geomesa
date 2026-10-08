/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.filter.interop

import org.locationtech.geomesa.filter.function.MurmurHashFunction._
import org.locationtech.geomesa.filter.function.{Convert2ViewerFunction, ProxyIdFunction, XZ2Function, Z2Function}
import org.locationtech.jts.geom.{Geometry, Point}

import java.util.{Date, UUID}

/**
 * Java-friendly access to GeoMesa filter function implementations without constructing
 * GeoTools expressions or features. Methods accept evaluated arguments; callers handle
 * null inputs, except for the nullable viewer ID. XZ2 returns null for empty geometries.
 */
object FilterFunctions {

  def murmurHash(value: String): Int = StringHashing(value)
  def murmurHash(value: Int): Int = IntegerHashing(value)
  def murmurHash(value: Long): Int = LongHashing(value)
  def murmurHash(value: Float): Int = FloatHashing(value)
  def murmurHash(value: Double): Int = DoubleHashing(value)
  def murmurHash(value: Date): Int = DateHashing(value)
  def murmurHash(value: Array[Byte]): Int = ByteHashing(value)
  def murmurHash(value: UUID): Int = UUIDHashing(value)

  def proxyId(id: String): Int = ProxyIdFunction.proxyId(id)
  def proxyId(id: UUID): Int = ProxyIdFunction.proxyId(id.getMostSignificantBits, id.getLeastSignificantBits)

  def z2(geom: Point): String = Z2Function.encode(geom)
  def xz2(geom: Geometry): String = XZ2Function.encode(geom)

  /** Encode a viewer record with a timestamp in milliseconds since the Unix epoch. */
  def convert2viewer(id: String, geom: Point, millis: Long): String = Convert2ViewerFunction.encode(id, geom, millis)
}
