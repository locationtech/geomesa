/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.parquet.io

import com.google.gson.stream.JsonWriter
import org.apache.parquet.variant.Variant
import org.apache.parquet.variant.Variant.Type
import org.locationtech.geomesa.utils.io.WithClose

import java.io.StringWriter
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate, LocalTime, ZoneOffset}
import java.util.Base64

/**
 * Utility object to serialize [[Variant]] values to JSON using Gson.
 */
object VariantJsonWriter {

  /**
   * Converts a Variant to a compact JSON string.
   *
   * @param variant the variant to serialize
   * @return compact JSON string
   */
  def toJson(variant: Variant): String = {
    val sw = new StringWriter()
    WithClose(new JsonWriter(sw)) { writer =>
      toJson(writer, variant)
    }
    sw.toString
  }

  /**
   * Writes a Variant to a JsonWriter.
   */
  def toJson(writer: JsonWriter, variant: Variant): Unit = {
    val variantType = variant.getType

    variantType match {
      case Type.NULL =>
        writer.nullValue()

      case Type.BOOLEAN =>
        writer.value(variant.getBoolean)

      case Type.BYTE | Type.SHORT | Type.INT | Type.LONG =>
        writer.value(variant.getLong)

      case Type.FLOAT =>
        val f = variant.getFloat
        if (f.isNaN || f.isInfinite) {
          writer.nullValue()  // JSON doesn't support NaN/Infinity
        } else {
          writer.value(f)
        }

      case Type.DOUBLE =>
        val d = variant.getDouble
        if (d.isNaN || d.isInfinite) {
          writer.nullValue()  // JSON doesn't support NaN/Infinity
        } else {
          writer.value(d)
        }

      case Type.DECIMAL4 | Type.DECIMAL8 | Type.DECIMAL16 =>
        writer.value(variant.getDecimal)

      case Type.STRING =>
        writer.value(variant.getString)

      case Type.BINARY =>
        val binary = variant.getBinary
        val bytes = new Array[Byte](binary.remaining())
        binary.duplicate().get(bytes)
        val base64 = Base64.getEncoder.encodeToString(bytes)
        writer.value(base64)

      case Type.DATE =>
        // convert days since epoch to ISO date string
        val daysSinceEpoch = variant.getInt
        val date = LocalDate.ofEpochDay(daysSinceEpoch.toLong)
        writer.value(date.format(DateTimeFormatter.ISO_LOCAL_DATE))

      case Type.TIME =>
        // convert microseconds since midnight to ISO time string
        val microsSinceMidnight = variant.getLong
        val time = LocalTime.ofNanoOfDay(microsSinceMidnight * 1000L)
        writer.value(time.format(DateTimeFormatter.ISO_LOCAL_TIME))

      case Type.TIMESTAMP_TZ | Type.TIMESTAMP_NANOS_TZ =>
        val micros = variant.getLong
        val instant = if (variantType == Type.TIMESTAMP_TZ) {
          Instant.ofEpochSecond(micros / 1000000L, (micros % 1000000L) * 1000L)
        } else {
          Instant.ofEpochSecond(micros / 1000000000L, micros % 1000000000L)
        }
        writer.value(instant.atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME))

      case Type.TIMESTAMP_NTZ | Type.TIMESTAMP_NANOS_NTZ =>
        val micros = variant.getLong
        val instant = if (variantType == Type.TIMESTAMP_NTZ) {
          Instant.ofEpochSecond(micros / 1000000L, (micros % 1000000L) * 1000L)
        } else {
          Instant.ofEpochSecond(micros / 1000000000L, micros % 1000000000L)
        }
        writer.value(instant.atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))

      case Type.ARRAY =>
        writer.beginArray()
        val size = variant.numArrayElements()
        var i = 0
        while (i < size) {
          toJson(writer, variant.getElementAtIndex(i))
          i += 1
        }
        writer.endArray()

      case Type.OBJECT =>
        writer.beginObject()
        val size = variant.numObjectElements()
        var i = 0
        while (i < size) {
          val field = variant.getFieldAtIndex(i)
          writer.name(field.key)
          toJson(writer, field.value)
          i += 1
        }
        writer.endObject()

      case _ =>
        throw new IllegalArgumentException(s"Unsupported variant type: $variantType")
    }
  }
}
