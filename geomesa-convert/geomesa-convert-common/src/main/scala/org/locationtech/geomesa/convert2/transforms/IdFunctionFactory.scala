/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php. 
 ***********************************************************************/

package org.locationtech.geomesa.convert2.transforms

import java.nio.charset.StandardCharsets
import java.util.{Date, UUID}

import com.google.common.hash.Hashing
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.jts.geom.{Geometry, Point}
import org.apache.commons.codec.binary.Base64
import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert2.transforms.TransformerFunction.NamedTransformerFunction
import org.locationtech.geomesa.curve.TimePeriod
import org.locationtech.geomesa.utils.uuid.Z3UuidGenerator

import scala.util.control.NonFatal

class IdFunctionFactory extends TransformerFunctionFactory with LazyLogging {

  override def functions: Seq[TransformerFunction] =
    Seq(string2Bytes, md5, uuid, uuidZ3, uuidZ3Centroid, base64, murmur3_32, murmur3_128)

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

  private val base64 = TransformerFunction.pure("base64") { args =>
    Base64.encodeBase64URLSafeString(args(0).asInstanceOf[Array[Byte]])
  }

  private val md5: TransformerFunction = new NamedTransformerFunction(Seq("md5"), pure = true) {
    private val hasher = Hashing.md5()
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any =
      hasher.hashBytes(args(0).asInstanceOf[Array[Byte]]).toString
  }

  private val murmur3_32: TransformerFunction = new NamedTransformerFunction(Seq("murmur3_32"), pure = true) {
    private val hasher = Hashing.murmur3_32()
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any =
      hasher.hashString(args(0).toString, StandardCharsets.UTF_8)
  }

  private val murmur3_128: TransformerFunction =
    new NamedTransformerFunction(Seq("murmur3_128", "murmur3_64"), pure = true) {
      private val hasher = Hashing.murmur3_128()
      override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any =
        hasher.hashString(args(0).toString, StandardCharsets.UTF_8).asLong()
    }
}
