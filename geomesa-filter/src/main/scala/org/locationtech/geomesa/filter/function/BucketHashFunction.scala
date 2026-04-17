/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.filter.function

import org.geotools.api.filter.expression.Expression
import org.geotools.filter.FunctionExpressionImpl
import org.geotools.filter.capability.FunctionNameImpl
import org.geotools.filter.capability.FunctionNameImpl._

import java.util.{Date, UUID}

class BucketHashFunction extends FunctionExpressionImpl(BucketHashFunction.Name) {

  import org.locationtech.geomesa.filter.function.MurmurHashFunction._

  private var modulo: Int = _

  override def setParameters(params: java.util.List[Expression]): Unit = {
    super.setParameters(params)
    modulo = getExpression(1).evaluate(null).asInstanceOf[Number].intValue()
  }

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
    Int.box((hash & Int.MaxValue) % modulo)
  }
}

object BucketHashFunction {

  val Name = new FunctionNameImpl(
    "bucketHash",
    classOf[java.lang.Integer],
    parameter("value", classOf[AnyRef]),
    parameter("modulo", classOf[Number]),
  )
}
