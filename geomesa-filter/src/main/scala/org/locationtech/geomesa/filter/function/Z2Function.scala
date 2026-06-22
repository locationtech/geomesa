/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.filter.function

import org.geotools.api.feature.simple.SimpleFeature
import org.geotools.api.filter.expression.{Expression, ExpressionVisitor}
import org.geotools.filter.FunctionExpressionImpl
import org.geotools.filter.capability.FunctionNameImpl
import org.geotools.filter.capability.FunctionNameImpl.parameter
import org.locationtech.geomesa.curve.Z2SFC
import org.locationtech.geomesa.filter.function.Z2Function.GetDefaultPoint
import org.locationtech.jts.geom.Point

/**
 * Function to calculate an XZ2 hex-encoded value
 */
class Z2Function extends FunctionExpressionImpl(Z2Function.FunctionName) {

  private var expression: Expression = _

  override def setParameters(params: java.util.List[Expression]): Unit = {
    super.setParameters(params)
    if (params.isEmpty) {
      expression = GetDefaultPoint
    } else {
      expression = getExpression(0)
    }
  }

  override def evaluate(o: AnyRef): AnyRef = {
    if (o == null) {
      return null
    }
    val value = expression.evaluate(o, classOf[Point])
    if (value == null) {
      return null
    }
    Z2SFC.hexEncode(value.getX, value.getY)
  }
}

object Z2Function {

  val FunctionName = new FunctionNameImpl("z2", classOf[String], parameter("geom", classOf[String], 0, 1))

  private object GetDefaultPoint extends Expression {

    override def evaluate(obj: Any): Point = obj match {
      case sf: SimpleFeature => sf.getDefaultGeometry.asInstanceOf[Point]
      case _ => null
    }

    override def evaluate[T](obj: Any, context: Class[T]): T = evaluate(obj).asInstanceOf[T] // only called by our code, above

    override def accept(visitor: ExpressionVisitor, extraData: Any): AnyRef = throw new UnsupportedOperationException()
  }
}
