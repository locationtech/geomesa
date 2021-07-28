/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php. 
 ***********************************************************************/

package org.locationtech.geomesa.convert2.transforms

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
import com.google.common.hash.Hashing
<<<<<<< HEAD
>>>>>>> 1e76dbd1e7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 0d4c68bdad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
=======
import com.google.common.hash.Hashing
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> ea96678625 (GEOMESA-3109 Json array to object converter function (#2788))
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.codec.binary.Base64
<<<<<<< HEAD
import org.apache.commons.codec.digest.MurmurHash3
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.codec.binary.Base64
import org.apache.commons.codec.digest.MurmurHash3
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
import org.locationtech.geomesa.convert2.transforms.TransformerFunction.NamedTransformerFunction
import org.locationtech.geomesa.curve.TimePeriod
import org.locationtech.geomesa.utils.index.ByteArrays
import org.locationtech.geomesa.utils.uuid.Z3UuidGenerator
import org.locationtech.jts.geom.{Geometry, Point}

import java.nio.charset.StandardCharsets
<<<<<<< HEAD
import java.security.MessageDigest
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> 1e76dbd1e7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 0d4c68bdad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> ea96678625 (GEOMESA-3109 Json array to object converter function (#2788))
import java.util.{Date, UUID}
import scala.util.control.NonFatal

class IdFunctionFactory extends TransformerFunctionFactory with LazyLogging {

  override def functions: Seq[TransformerFunction] =
    Seq(string2Bytes, md5, uuid, uuidZ3, uuidZ3Centroid, base64, murmur3_32, murmur3_64, murmur3_128)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
=======

  private val murmur3_128Hashing = Hashing.murmur3_128()
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> ea96678625 (GEOMESA-3109 Json array to object converter function (#2788))

<<<<<<< HEAD
  private val murmur3_128Hashing = Hashing.murmur3_128()
<<<<<<< HEAD
>>>>>>> 1e76dbd1e7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 0d4c68bdad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)

=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
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
<<<<<<< HEAD
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
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
    private val hasher = Hashing.md5()
    override def apply(args: Array[AnyRef]): AnyRef =
      hasher.hashBytes(args(0).asInstanceOf[Array[Byte]]).toString
<<<<<<< HEAD
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
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

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
  private val murmur3_128: TransformerFunction =
    new NamedTransformerFunction(Seq("murmur3_128", "murmur3_64"), pure = true) {
      private val hasher = Hashing.murmur3_128()
      override def apply(args: Array[AnyRef]): AnyRef =
        Long.box(hasher.hashString(args(0).toString, StandardCharsets.UTF_8).asLong())
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4aef7a70f3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0d80bae0c6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 64d8177ac0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 7a670f84c4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 097b5a0858 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 149b7a7809 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2c3111e686 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c44517c361 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 235691f96a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 686339d050 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 097b5a0858 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c361 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 235691f96a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7a670f84c4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f3a49e0825 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 31b03236c6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6289007008 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 097b5a0858 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c44517c361 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 149b7a7809 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 2c3111e686 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c361 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0f4c829f25 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9759ddc1b5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 455aae09d3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 235691f96a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1e76dbd1e7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 235691f96a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 455aae09d3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 9759ddc1b5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 4aef7a70f3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0d80bae0c6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 64d8177ac0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 7a670f84c4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 097b5a0858 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 149b7a7809 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2c3111e686 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c361 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 686339d050 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f3a49e0825 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 31b03236c6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6289007008 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 0f4c829f25 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 235691f96a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0d4c68bdad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 55b43ae566 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 94213b24aa (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cb7d3570f5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dbc712b84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 89971e000d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8caee74520 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4a51d3f071 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0166e9455b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 354c37f8b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 25e967804c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9c337194ec (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a2ac294bf3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 35b3ecb03d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
  }

  private val murmur3_32: TransformerFunction = new NamedTransformerFunction(Seq("murmur3_32"), pure = true) {
    private val hasher = Hashing.murmur3_32()
    override def apply(args: Array[AnyRef]): AnyRef =
      hasher.hashString(args(0).toString, StandardCharsets.UTF_8)
  }

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> ea96678625 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> eccc16ddf9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 063b0f26f0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 71edb3b56e (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> d5f1bdf64f (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 1bc88f7e23 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 84bc7e0e2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> a38d1a4cc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> f586618a0c (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> c0dc422e29 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 02ce9d8126 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> cd8248bbdc (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 81b8eb5aeb (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 09c8a6d2fd (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
=======
>>>>>>> c83e8187d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 7a670e5d35 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 8cb3591e39 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 62ff7eb02c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 448369e575 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a76cfdacc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 793ec81151 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7982d54d93 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4be1359e88 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 63203d9a42 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e8c33ac76a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 38f95b1602 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 36a5acc573 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 79e839e899 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7558f8f4ee (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
=======
>>>>>>> c83e8187d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7a6dd271d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> fd776cb831 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> e014d8c87 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 8cb3591e39 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6ba18529e (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 62ff7eb02c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> e7b61a536 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 448369e575 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 0df4f16d9 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a76cfdacc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 4350edc8f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 793ec81151 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 008feb67a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7982d54d93 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 4be1359e88 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 63203d9a42 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 4bd9eb4df (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e8c33ac76a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 04d469083d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 38f95b1602 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> 74661c314 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2db4ecdc72 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 74661c314 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0d4c68bda (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 919559e486 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 36a5acc573 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 55b43ae56 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 79e839e899 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 94213b24a (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2db4ecdc72 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7558f8f4ee (GEOMESA-3109 Json array to object converter function (#2788))
  private val murmur3_128: TransformerFunction =
    new NamedTransformerFunction(Seq("murmur3_128", "murmur3_64"), pure = true) {
      private val hasher = Hashing.murmur3_128()
      override def apply(args: Array[AnyRef]): AnyRef =
        Long.box(hasher.hashString(args(0).toString, StandardCharsets.UTF_8).asLong())
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> dd5d6434b0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4aef7a70f3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0d80bae0c6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 64d8177ac0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 7a670f84c4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 097b5a0858 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 149b7a7809 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2c3111e686 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c44517c361 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 235691f96a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> f586618a0c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 686339d050 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 097b5a0858 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c361 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 235691f96a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 063b0f26f0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> f586618a0c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7a670f84c4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f3a49e0825 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 31b03236c6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6289007008 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 097b5a0858 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c44517c361 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 149b7a7809 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d5f1bdf64f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c0dc422e29 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2c3111e686 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c361 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0f4c829f25 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9759ddc1b5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 455aae09d3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 235691f96a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7a670e5d35 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> fd776cb831 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
<<<<<<< HEAD
>>>>>>> 919559e486 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 36a5acc573 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
=======
>>>>>>> 008feb67a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e7b61a536 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 008feb67a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> c0dc422e29 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 448369e575 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 008feb67a (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7982d54d93 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 04d469083d (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 04d469083d (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 04d469083d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> f586618a0c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 09c8a6d2fd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 04d469083d (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 38f95b1602 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7982d54d93 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> cd8248bbdc (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 81b8eb5aeb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e8c33ac76a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> ea96678625 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> eccc16ddf9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 063b0f26f0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 71edb3b56e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d5f1bdf64f (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 1bc88f7e23 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 84bc7e0e2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a38d1a4cc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f586618a0c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c0dc422e29 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 02ce9d8126 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cd8248bbdc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 81b8eb5aeb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 09c8a6d2fd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
<<<<<<< HEAD
>>>>>>> 7a6dd271d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> fd776cb831 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> e014d8c87 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 8cb3591e39 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
<<<<<<< HEAD
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0df4f16d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 6ba18529e (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 62ff7eb02c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> e7b61a536 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 448369e575 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 0df4f16d9 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a76cfdacc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4350edc8f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 793ec81151 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 008feb67a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7982d54d93 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 4be1359e88 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 63203d9a42 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 4bd9eb4df (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e8c33ac76a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 04d469083d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 38f95b1602 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 0d4c68bda (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 919559e486 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 36a5acc573 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 55b43ae56 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 79e839e899 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 94213b24a (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2db4ecdc72 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7558f8f4ee (GEOMESA-3109 Json array to object converter function (#2788))
  // we've had some confusion around the names of these functions - the original function was murmur3_64,
  // which was then incorrectly renamed to murmur3_128. currently both these functions only return the first 64
  // bits of a 128 bit hash. the full 128-bit hash is now called murmurHash3 to avoid name conflicts
  private val murmur3_64 =
    TransformerFunction.pure("murmur3_128", "murmur3_64") { args =>
      val hash = args(0) match {
        case s: String => murmur3_128Hashing.hashBytes(s.getBytes(StandardCharsets.UTF_8))
        case b: Array[Byte] => murmur3_128Hashing.hashBytes(b)
        case a => throw new IllegalArgumentException(s"Expected String or byte[] but got: $a")
      }
      Long.box(hash.asLong()) // asLong gets only the first 64 bits even though the hash has 128
    }

  private val murmur3_128 =
    TransformerFunction.pure("murmurHash3") { args =>
      args(0) match {
        case s: String => murmur3_128Hashing.hashBytes(s.getBytes(StandardCharsets.UTF_8)).toString // toString results in hex
        case b: Array[Byte] => murmur3_128Hashing.hashBytes(b).toString // toString results in hex
        case a => throw new IllegalArgumentException(s"Expected String or byte[] but got: $a")
      }
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d5f1bdf64f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1bc88f7e23 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 84bc7e0e2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a38d1a4cc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f586618a0c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c0dc422e29 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 02ce9d8126 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cd8248bbdc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 81b8eb5aeb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 09c8a6d2fd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> fd776cb831 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8cb3591e39 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 62ff7eb02c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> e7b61a536 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 448369e575 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 793ec81151 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7982d54d93 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4be1359e88 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 63203d9a42 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e8c33ac76a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 38f95b1602 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 36a5acc573 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 79e839e899 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7558f8f4ee (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e7b61a536 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0df4f16d9 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a76cfdacc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 4350edc8f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 793ec81151 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 008feb67a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7982d54d93 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 4be1359e88 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 63203d9a42 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 4bd9eb4df (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e8c33ac76a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 04d469083d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 38f95b1602 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 919559e486 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 36a5acc573 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 79e839e899 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 2db4ecdc72 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7558f8f4ee (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 0d4c68bdad (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 448369e575 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 008feb67a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4be1359e88 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 09c8a6d2fd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8cb3591e39 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 38f95b1602 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 79e839e899 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> eccc16ddf9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 71edb3b56e (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 063b0f26f0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 71edb3b56e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d5f1bdf64f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1bc88f7e23 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 84bc7e0e2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a38d1a4cc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f586618a0c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c0dc422e29 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 02ce9d8126 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 09c8a6d2fd (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 79e839e899 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> e014d8c87 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 8cb3591e39 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 6ba18529e (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 62ff7eb02c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> e7b61a536 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 448369e575 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 0df4f16d9 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a76cfdacc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 4350edc8f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 793ec81151 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 008feb67a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7982d54d93 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 4be1359e88 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 63203d9a42 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 04d469083d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 38f95b1602 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 55b43ae56 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 79e839e899 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 94213b24a (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2db4ecdc72 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7558f8f4ee (GEOMESA-3109 Json array to object converter function (#2788))
  private val murmur3_128: TransformerFunction =
    new NamedTransformerFunction(Seq("murmur3_128", "murmur3_64"), pure = true) {
      private val hasher = Hashing.murmur3_128()
      override def apply(args: Array[AnyRef]): AnyRef =
        Long.box(hasher.hashString(args(0).toString, StandardCharsets.UTF_8).asLong())
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 02ce9d8126 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cd8248bbdc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 81b8eb5aeb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 09c8a6d2fd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8cb3591e39 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 62ff7eb02c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 448369e575 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a76cfdacc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 793ec81151 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 4be1359e88 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
=======
>>>>>>> 2db4ecdc72 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7558f8f4ee (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 79e839e899 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 04d469083d (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 38f95b1602 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4bd9eb4df (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e8c33ac76a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 63203d9a42 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 008feb67a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 008feb67a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 71edb3b56e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d5f1bdf64f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1bc88f7e23 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 84bc7e0e2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a38d1a4cc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f586618a0c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c0dc422e29 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> e7b61a536 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 448369e575 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 0df4f16d9 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a76cfdacc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 4350edc8f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 793ec81151 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 55b43ae566 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 063b0f26f0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6ba18529e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 94213b24a (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2db4ecdc72 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 94213b24aa (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 62ff7eb02c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 2db4ecdc72 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7558f8f4ee (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> cb7d3570f5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 448369e575 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d5f1bdf64f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c0dc422e29 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e7b61a536 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> dbc712b84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 448369e575 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 04d469083d (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 89971e000d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 8caee74520 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 4a51d3f071 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 0166e9455b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 354c37f8b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 7982d54d93 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 4be1359e88 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 63203d9a42 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 25e967804c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9c337194ec (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> e8c33ac76a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a2ac294bf3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 35b3ecb03d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
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
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> ea96678625 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> eccc16ddf9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 063b0f26f0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 71edb3b56e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d5f1bdf64f (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c0dc422e29 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 1bc88f7e23 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 84bc7e0e2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a38d1a4cc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> f586618a0c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> c0dc422e29 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 02ce9d8126 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> cd8248bbdc (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 81b8eb5aeb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 09c8a6d2fd (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
<<<<<<< HEAD
>>>>>>> c83e8187d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7a670e5d35 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a6dd271d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> fd776cb831 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> e014d8c87 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 8cb3591e39 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6ba18529e (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 62ff7eb02c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> e7b61a536 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 448369e575 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
<<<<<<< HEAD
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 0df4f16d9 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a76cfdacc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4350edc8f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 793ec81151 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 008feb67a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7982d54d93 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 4be1359e88 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 63203d9a42 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 4bd9eb4df (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e8c33ac76a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 04d469083d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 38f95b1602 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 74661c314 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 0d4c68bda (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 919559e486 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 36a5acc573 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55b43ae56 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 79e839e899 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 94213b24a (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2db4ecdc72 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7558f8f4ee (GEOMESA-3109 Json array to object converter function (#2788))
    }
}
