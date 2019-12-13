/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.filter.function

import org.geotools.filter.FunctionExpressionImpl
import org.geotools.filter.capability.FunctionNameImpl
import org.opengis.feature.simple.SimpleFeature

class FastProperty extends FunctionExpressionImpl(FastProperty.Name) {

  private var idx: Int = -1

  def this(i: Int) = {
    this()
    idx = i
  }

  override def evaluate(o: AnyRef): AnyRef = {
    if (idx == -1) {
      idx = getExpression(0).evaluate(null).asInstanceOf[Long].toInt
    }
    o.asInstanceOf[SimpleFeature].getAttribute(idx)
  }
}

object FastProperty {
  val Name = new FunctionNameImpl(
    "fastproperty",
    FunctionNameImpl.parameter("propertyValue", classOf[Object]),
    FunctionNameImpl.parameter("propertyIndex", classOf[Integer])
  )
}
