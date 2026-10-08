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
import org.locationtech.geomesa.curve.XZ2SFC
import org.locationtech.jts.geom.Geometry

/**
 * Function to calculate an XZ2 hex-encoded value
 */
class XZ2Function extends FunctionExpressionImpl(XZ2Function.FunctionName) {

  override def evaluate(o: AnyRef): AnyRef = {
    val value = getExpression(0).evaluate(o, classOf[Geometry])
    if (value == null) {
      return null
    }
    XZ2Function.encode(value)
  }
}

object XZ2Function {

  val FunctionName = new FunctionNameImpl("xz2", classOf[String], parameter("geom", classOf[Geometry]))

  private[filter] def encode(geom: Geometry): String = {
    val env = geom.getEnvelopeInternal
    if (env.isNull) { null } else {
      XZ2SFC.hexEncode(env.getMinX, env.getMinY, env.getMaxX, env.getMaxY)
    }
  }
}
