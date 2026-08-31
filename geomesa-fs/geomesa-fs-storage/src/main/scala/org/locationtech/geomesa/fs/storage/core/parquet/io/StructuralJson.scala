/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.parquet.io

import com.google.gson.JsonElement
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit

import java.nio.ByteBuffer
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime, OffsetDateTime, ZoneOffset}
import java.util.{Base64, UUID}

/**
 * Shared helpers for reading/writing structural JSON attributes. Leaf value formats (dates, times,
 * timestamps, binary, uuids) mirror [[VariantJsonWriter]] so that structural and variant JSON round-trip
 * consistently across the parquet and iceberg backends.
 */
object StructuralJson {

  private val encoder = Base64.getEncoder
  private val decoder = Base64.getDecoder

  /**
   * Serializes a gson element to a compact JSON string
   */
  def compact(element: JsonElement): String = element.toString

  // ---- binary ----

  def bytesToJson(bytes: Array[Byte]): String = encoder.encodeToString(bytes)

  def bytesToJson(bb: ByteBuffer): String = {
    val dup = bb.duplicate()
    val bytes = Array.ofDim[Byte](dup.remaining())
    dup.get(bytes)
    encoder.encodeToString(bytes)
  }

  def jsonToBytes(value: String): Array[Byte] = decoder.decode(value)

  // ---- decimal ----

  /**
   * Encodes a decimal as the two's-complement, sign-extended, fixed-length byte array used by
   * parquet FIXED_LEN_BYTE_ARRAY decimals. Mirrors iceberg's DecimalUtil.toReusedFixLengthBytes.
   *
   * @param value decimal value
   * @param scale schema scale - the value is rescaled to match, as required by the fixed encoding
   * @param length fixed byte length from the schema
   */
  def decimalToFixedBytes(value: java.math.BigDecimal, scale: Int, length: Int): Array[Byte] = {
    val scaled = value.setScale(scale)
    val unscaled = scaled.unscaledValue().toByteArray
    if (unscaled.length == length) { unscaled } else {
      val buf = Array.ofDim[Byte](length)
      val fill = if (scaled.signum() < 0) { 0xFF.toByte } else { 0x00.toByte }
      val offset = length - unscaled.length
      var i = 0
      while (i < length) {
        buf(i) = if (i < offset) { fill } else { unscaled(i - offset) }
        i += 1
      }
      buf
    }
  }

  // ---- uuid ----

  def uuidToBytes(uuid: UUID): Array[Byte] = {
    val bb = ByteBuffer.wrap(Array.ofDim[Byte](16))
    bb.putLong(uuid.getMostSignificantBits)
    bb.putLong(uuid.getLeastSignificantBits)
    bb.array()
  }

  // ---- date (days since epoch) ----

  def dateToJson(epochDay: Long): String = LocalDate.ofEpochDay(epochDay).format(DateTimeFormatter.ISO_LOCAL_DATE)
  def dateToJson(date: LocalDate): String = date.format(DateTimeFormatter.ISO_LOCAL_DATE)
  def jsonToEpochDay(value: String): Int = LocalDate.parse(value, DateTimeFormatter.ISO_LOCAL_DATE).toEpochDay.toInt

  // ---- time (of day, in the given unit) ----

  def timeToJson(value: Long, unit: TimeUnit): String =
    LocalTime.ofNanoOfDay(value * nanosPerUnit(unit)).format(DateTimeFormatter.ISO_LOCAL_TIME)
  def timeToJson(time: LocalTime): String = time.format(DateTimeFormatter.ISO_LOCAL_TIME)
  def jsonToTime(value: String, unit: TimeUnit): Long =
    LocalTime.parse(value, DateTimeFormatter.ISO_LOCAL_TIME).toNanoOfDay / nanosPerUnit(unit)

  // ---- timestamp (in the given unit, adjusted to utc or not) ----

  def timestampToJson(value: Long, unit: TimeUnit, utc: Boolean): String = {
    val offset = toInstant(value, unit).atOffset(ZoneOffset.UTC)
    if (utc) {
      offset.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
    } else {
      offset.toLocalDateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
    }
  }
  def timestampToJson(value: OffsetDateTime): String = value.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
  def timestampToJson(value: LocalDateTime): String = value.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)

  def jsonToTimestamp(value: String, unit: TimeUnit, utc: Boolean): Long = {
    val instant =
      if (utc) {
        OffsetDateTime.parse(value, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant
      } else {
        LocalDateTime.parse(value, DateTimeFormatter.ISO_LOCAL_DATE_TIME).toInstant(ZoneOffset.UTC)
      }
    fromInstant(instant, unit)
  }

  private def nanosPerUnit(unit: TimeUnit): Long = unit match {
    case TimeUnit.MILLIS => 1000000L
    case TimeUnit.MICROS => 1000L
    case TimeUnit.NANOS  => 1L
  }

  private def toInstant(value: Long, unit: TimeUnit): Instant = unit match {
    case TimeUnit.MILLIS => Instant.ofEpochMilli(value)
    case TimeUnit.MICROS => Instant.ofEpochSecond(value / 1000000L, (value % 1000000L) * 1000L)
    case TimeUnit.NANOS  => Instant.ofEpochSecond(value / 1000000000L, value % 1000000000L)
  }

  private def fromInstant(instant: Instant, unit: TimeUnit): Long = unit match {
    case TimeUnit.MILLIS => instant.toEpochMilli
    case TimeUnit.MICROS => instant.getEpochSecond * 1000000L + instant.getNano / 1000L
    case TimeUnit.NANOS  => instant.getEpochSecond * 1000000000L + instant.getNano
  }
}
