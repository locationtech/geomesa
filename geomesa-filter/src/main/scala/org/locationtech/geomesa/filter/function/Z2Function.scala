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
import org.geotools.filter.capability.FunctionNameImpl.parameter
import org.locationtech.geomesa.curve.Z2SFC
import org.locationtech.jts.geom.Point

/**
 * Function to calculate a Z2 hex-encoded value
 */
class Z2Function extends FunctionExpressionImpl(Z2Function.FunctionName) {

  override def evaluate(o: AnyRef): AnyRef = {
    val value = getExpression(0).evaluate(o, classOf[Point])
    if (value == null) {
      return null
    }
    Z2Function.encode(value)
  }
}

object Z2Function {

  val FunctionName = new FunctionNameImpl("z2", classOf[String], parameter("geom", classOf[Point]))

  private[filter] def encode(point: Point): String = Z2SFC.hexEncode(point.getX, point.getY)
}
