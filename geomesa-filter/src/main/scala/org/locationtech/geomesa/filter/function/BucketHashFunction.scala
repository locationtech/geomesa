/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.filter.function

import org.geotools.filter.FunctionExpressionImpl
import org.geotools.filter.capability.FunctionNameImpl
import org.geotools.filter.capability.FunctionNameImpl._

class BucketHashFunction extends FunctionExpressionImpl(BucketHashFunction.Name) {

  import org.locationtech.geomesa.filter.function.MurmurHashFunction._

  override def evaluate(o: AnyRef): AnyRef = {
    val value = getExpression(0).evaluate(o)
    if (value == null) {
      return null
    }
    val modulo = getExpression(1).evaluate(o).asInstanceOf[Number]
    if (modulo == null) { null } else { Int.box((hash(value) & Int.MaxValue) % modulo.intValue()) }
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
