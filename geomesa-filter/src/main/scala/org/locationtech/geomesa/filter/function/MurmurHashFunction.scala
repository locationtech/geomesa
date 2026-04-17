/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.filter.function

import org.apache.commons.codec.digest.MurmurHash3
import org.geotools.filter.FunctionExpressionImpl
import org.geotools.filter.capability.FunctionNameImpl
import org.geotools.filter.capability.FunctionNameImpl._

import java.nio.charset.StandardCharsets
import java.util.{Date, UUID}
import scala.reflect.ClassTag

/**
 * Hash function, follows the Apache Icebergs hash specification - https://iceberg.apache.org/spec/#appendix-b-32-bit-hash-requirements
 */
class MurmurHashFunction extends FunctionExpressionImpl(MurmurHashFunction.Name) {

  import org.locationtech.geomesa.filter.function.MurmurHashFunction._

  override def evaluate(o: AnyRef): AnyRef = {
    if (o == null) {
      return null
    }
    val value = getExpression(0).evaluate(o)
    if (value == null) {
      return null
    }
    val hash = value match {
      case v: String => StringHashing(v)
      case v: Integer => IntegerHashing(v)
      case v: java.lang.Long => LongHashing(v)
      case v: java.lang.Float => FloatHashing(v)
      case v: java.lang.Double => DoubleHashing(v)
      case v: Date => DateHashing(v)
      case v: Array[Byte] => ByteHashing(v)
      case v: UUID => UUIDHashing(v)
      case _ => StringHashing(o.toString)
    }
    Int.box(hash)
  }
}

object MurmurHashFunction {

  val Name = new FunctionNameImpl(
    "murmurHash",
    classOf[java.lang.Integer],
    parameter("value", classOf[AnyRef])
  )

  val Hashers: Seq[Hashing[_ <: AnyRef]] = Seq(
    StringHashing,
    IntegerHashing,
    LongHashing,
    FloatHashing,
    DoubleHashing,
    DateHashing,
    ByteHashing,
    UUIDHashing,
  )

  abstract class Hashing[T: ClassTag] extends (T => Int) {
    val binding: Class[T] = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
  }

  object StringHashing extends Hashing[String] {
    // string	hashBytes(utf8Bytes(v))
    override def apply(v1: String): Int = {
      val bytes = v1.getBytes(StandardCharsets.UTF_8)
      MurmurHash3.hash32x86(bytes, 0, bytes.length, 0)
    }
  }

  object IntegerHashing extends Hashing[Integer]{
    // int	hashLong(long(v))
    override def apply(v1: Integer): Int = LongHashing(v1.toLong)
  }

  object LongHashing extends Hashing[java.lang.Long]{
    // long	hashBytes(littleEndianBytes(v))
    override def apply(v1: java.lang.Long): Int =  MurmurHash3.hash32(java.lang.Long.reverseBytes(v1.toLong), 0)
  }

  object FloatHashing extends Hashing[java.lang.Float]{
    // float	hashLong(doubleToLongBits(double(v))
    override def apply(v1: java.lang.Float): Int = LongHashing(java.lang.Double.doubleToLongBits(v1.toDouble))
  }

  object DoubleHashing extends Hashing[java.lang.Double]{
    // double	hashLong(doubleToLongBits(v))
    override def apply(v1: java.lang.Double): Int = LongHashing(java.lang.Double.doubleToLongBits(v1.toDouble))
  }

  object DateHashing extends Hashing[Date]{
    // timestamp	hashLong(microsecsFromUnixEpoch(v))
    override def apply(v1: Date): Int = LongHashing(v1.getTime * 1000)
  }

  object ByteHashing extends Hashing[Array[Byte]]{
    // binary	hashBytes(v)
    override def apply(v1: Array[Byte]): Int = MurmurHash3.hash32x86(v1, 0, v1.length, 0)
  }

  object UUIDHashing extends Hashing[UUID]{
    // uuid	hashBytes(uuidBytes(v)
    override def apply(v1: UUID): Int = MurmurHash3.hash32(v1.getMostSignificantBits, v1.getLeastSignificantBits, 0)
  }
}
