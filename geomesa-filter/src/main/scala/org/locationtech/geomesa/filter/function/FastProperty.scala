/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.filter.function

import org.geotools.filter.FunctionExpressionImpl
import org.geotools.filter.capability.FunctionNameImpl
import org.geotools.filter.capability.FunctionNameImpl._
import org.opengis.feature.simple.SimpleFeature

class FastProperty extends FunctionExpressionImpl(FastProperty.Name) {

  private var idx: Int = -1

  def this(i: Int) = {
    this()
    idx = i
  }

  override def evaluate(o: AnyRef): AnyRef = {
    if (idx == -1) {
      val tmp = getExpression(0).evaluate(null)
      idx =
        tmp match {
          case long: java.lang.Long => long.toInt
          case _ => tmp.asInstanceOf[java.lang.Integer]
        }
    }
    o.asInstanceOf[SimpleFeature].getAttribute(idx)
  }
}

object FastProperty {
  val Name = new FunctionNameImpl(
    "fastproperty",
    parameter("propertyValue", classOf[Object]),
    parameter("propertyIndex", classOf[Integer])
  )
}
