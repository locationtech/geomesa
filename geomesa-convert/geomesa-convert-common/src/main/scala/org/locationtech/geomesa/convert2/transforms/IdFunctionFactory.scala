/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0 
 ***********************************************************************/

package org.locationtech.geomesa.convert2.transforms

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.codec.digest.MurmurHash3
import org.locationtech.geomesa.convert2.transforms.TransformerFunction.NamedTransformerFunction
import org.locationtech.geomesa.curve.TimePeriod
import org.locationtech.geomesa.utils.index.ByteArrays
import org.locationtech.geomesa.utils.uuid.Z3UuidGenerator
import org.locationtech.jts.geom.{Geometry, Point}

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.{Date, UUID}
import scala.util.control.NonFatal

class IdFunctionFactory extends TransformerFunctionFactory with LazyLogging {

  override def functions: Seq[TransformerFunction] =
    Seq(stringToBytes, md5, uuid, uuidZ3, uuidZ3Centroid, murmur3_32, murmur3_64, murmur3_128)

  private val stringToBytes = TransformerFunction("stringToBytes", "string2bytes") {
    args => args(0).asInstanceOf[String].getBytes(StandardCharsets.UTF_8)
  }

  private val uuid = TransformerFunction("uuid") { args =>
    if (args == null || args.isEmpty) {
      UUID.randomUUID().toString
    } else if (args(0) == null) {
      null
    } else {
      UUID.fromString(args(0).toString)
    }
  }

  private val uuidZ3 = TransformerFunction("uuidZ3") { args =>
    val geom = args(0).asInstanceOf[Point]
    val date = args(1).asInstanceOf[Date]
    val interval = TimePeriod.withName(args(2).asInstanceOf[String])
    try { Z3UuidGenerator.createUuid(geom, date.getTime, interval).toString } catch {
      case NonFatal(e) =>
        logger.warn(s"Invalid z3 values for UUID: $geom $date $interval: $e")
        UUID.randomUUID().toString
    }
  }

  private val uuidZ3Centroid = TransformerFunction("uuidZ3Centroid") { args =>
    val geom = args(0).asInstanceOf[Geometry]
    val date = args(1).asInstanceOf[Date]
    val interval = TimePeriod.withName(args(2).asInstanceOf[String])
    try { Z3UuidGenerator.createUuid(geom, date.getTime, interval).toString } catch {
      case NonFatal(e) =>
        logger.warn(s"Invalid z3 values for UUID: $geom $date $interval: $e")
        UUID.randomUUID().toString
    }
  }

  private val md5: TransformerFunction = new NamedTransformerFunction(Seq("md5"), pure = true) {
    private val hashers = new ThreadLocal[MessageDigest]() {
      override def initialValue(): MessageDigest = MessageDigest.getInstance("MD5")
    }
    override def apply(args: Array[AnyRef]): AnyRef = {
      val bytes = args(0) match {
        case s: String => s.getBytes(StandardCharsets.UTF_8)
        case b: Array[Byte] => b
        case a => throw new IllegalArgumentException(s"Expected String or byte[] but got: $a")
      }
      ByteArrays.toHex(hashers.get.digest(bytes))
    }
  }

  private val murmur3_32: TransformerFunction = new NamedTransformerFunction(Seq("murmur3_32"), pure = true) {
    override def apply(args: Array[AnyRef]): AnyRef = {
      val bytes = args(0) match {
        case s: String => s.getBytes(StandardCharsets.UTF_8)
        case b: Array[Byte] => b
        case a => throw new IllegalArgumentException(s"Expected String or byte[] but got: $a")
      }
      Int.box(MurmurHash3.hash32x86(bytes, 0, bytes.length, 0))
    }
  }

  // we've had some confusion around the names of these functions - the original function was murmur3_64,
  // which was then incorrectly renamed to murmur3_128. currently both these functions only return the first 64
  // bits of a 128 bit hash. the full 128-bit hash is now called murmurHash3 to avoid name conflicts
  private val murmur3_64 =
    TransformerFunction.pure("murmur3_128", "murmur3_64") { args =>
      val bytes = args(0) match {
        case s: String => s.getBytes(StandardCharsets.UTF_8)
        case b: Array[Byte] => b
        case a => throw new IllegalArgumentException(s"Expected String or byte[] but got: $a")
      }
      Long.box(MurmurHash3.hash128x64(bytes, 0, bytes.length, 0).head)
    }

  private val murmur3_128 =
    TransformerFunction.pure("murmurHash3") { args =>
      val bytes = args(0) match {
        case s: String => s.getBytes(StandardCharsets.UTF_8)
        case b: Array[Byte] => b
        case a => throw new IllegalArgumentException(s"Expected String or byte[] but got: $a")
      }
      // mimic guava little-endian output
      val sb = new StringBuilder(32)
      MurmurHash3.hash128x64(bytes, 0, bytes.length, 0).foreach { hash =>
        var i = 0
        while (i < 64) {
          sb.append(ByteArrays.toHex(((hash >> i) & 0xff).asInstanceOf[Byte]))
          i += 8
        }
      }
      sb.toString
    }
}
