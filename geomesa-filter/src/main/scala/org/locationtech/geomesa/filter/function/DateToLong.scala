/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.filter.function

import java.{lang => jl}

import org.geotools.filter.FunctionExpressionImpl
import org.geotools.filter.capability.FunctionNameImpl
import org.geotools.filter.capability.FunctionNameImpl._
import org.opengis.feature.simple.SimpleFeature

class DateToLong
  extends FunctionExpressionImpl(
    new FunctionNameImpl(
      "dateToLong",
      classOf[java.lang.Long],
      parameter("dtg", classOf[java.util.Date])
    )
  ) {

  def evaluate(feature: SimpleFeature): jl.Object =
    jl.Long.valueOf(getExpression(0).evaluate(feature).asInstanceOf[java.util.Date].getTime)

  override def evaluate(o: jl.Object): jl.Object =
    jl.Long.valueOf(getExpression(0).evaluate(o).asInstanceOf[java.util.Date].getTime)

}
