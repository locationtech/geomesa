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
import org.locationtech.geomesa.curve.XZ2SFC
import org.locationtech.geomesa.filter.function.XZ2Function.GetDefaultGeometry
import org.locationtech.jts.geom.Geometry

/**
 * Function to calculate an XZ2 hex-encoded value
 */
class XZ2Function extends FunctionExpressionImpl(XZ2Function.FunctionName) {

  private var expression: Expression = _

  override def setParameters(params: java.util.List[Expression]): Unit = {
    super.setParameters(params)
    if (params.isEmpty) {
      expression = GetDefaultGeometry
    } else {
      expression = getExpression(0)
    }
  }

  override def evaluate(o: AnyRef): AnyRef = {
    if (o == null) {
      return null
    }
    val value = expression.evaluate(o, classOf[Geometry])
    if (value == null) {
      return null
    }
    val env = value.getEnvelopeInternal
    if (env.isNull) {
      return null
    }
    XZ2SFC.hexEncode(XZ2SFC.index(env.getMinX, env.getMinY, env.getMaxX, env.getMaxY))
  }
}

object XZ2Function {

  val FunctionName = new FunctionNameImpl("xz2", classOf[String], parameter("geom", classOf[String], 0, 1))

  private object GetDefaultGeometry extends Expression {

    override def evaluate(obj: Any): Geometry = obj match {
      case sf: SimpleFeature => sf.getDefaultGeometry.asInstanceOf[Geometry]
      case _ => null
    }

    override def evaluate[T](obj: Any, context: Class[T]): T = evaluate(obj).asInstanceOf[T] // only called by our code, above

    override def accept(visitor: ExpressionVisitor, extraData: Any): AnyRef = throw new UnsupportedOperationException()
  }
}
