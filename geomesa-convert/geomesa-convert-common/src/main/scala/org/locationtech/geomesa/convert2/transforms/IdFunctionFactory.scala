/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php. 
 ***********************************************************************/

package org.locationtech.geomesa.convert2.transforms

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.codec.binary.Base64
<<<<<<< HEAD
import org.apache.commons.codec.digest.MurmurHash3
=======
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
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
    Seq(string2Bytes, md5, uuid, uuidZ3, uuidZ3Centroid, base64, murmur3_32, murmur3_64, murmur3_128)

  private val string2Bytes = TransformerFunction("string2bytes", "stringToBytes") {
    args => args(0).asInstanceOf[String].getBytes(StandardCharsets.UTF_8)
  }

  private val uuid = TransformerFunction("uuid") { _ => UUID.randomUUID().toString }

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

  @deprecated("Replaced with base64Encode")
  private val base64 = TransformerFunction.pure("base64") { args =>
    Base64.encodeBase64URLSafeString(args(0).asInstanceOf[Array[Byte]])
  }

  private val md5: TransformerFunction = new NamedTransformerFunction(Seq("md5"), pure = true) {
<<<<<<< HEAD
    private val hasher = MessageDigest.getInstance("MD5")
    override def apply(args: Array[AnyRef]): AnyRef = {
      val bytes = args(0) match {
        case s: String => s.getBytes(StandardCharsets.UTF_8)
        case b: Array[Byte] => b
        case a => throw new IllegalArgumentException(s"Expected String or byte[] but got: $a")
      }
      ByteArrays.toHex(hasher.digest(bytes))
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
=======
    private val hasher = Hashing.md5()
    override def apply(args: Array[AnyRef]): AnyRef =
      hasher.hashBytes(args(0).asInstanceOf[Array[Byte]]).toString
  }

  private val murmur3_32: TransformerFunction = new NamedTransformerFunction(Seq("murmur3_32"), pure = true) {
    private val hasher = Hashing.murmur3_32()
    override def apply(args: Array[AnyRef]): AnyRef =
      hasher.hashString(args(0).toString, StandardCharsets.UTF_8)
  }

  private val murmur3_128: TransformerFunction =
    new NamedTransformerFunction(Seq("murmur3_128", "murmur3_64"), pure = true) {
      private val hasher = Hashing.murmur3_128()
      override def apply(args: Array[AnyRef]): AnyRef =
        Long.box(hasher.hashString(args(0).toString, StandardCharsets.UTF_8).asLong())
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
    }
}
