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
import org.geotools.filter.capability.FunctionNameImpl._

class DateToLong extends FunctionExpressionImpl(DateToLong.Name) {
  override def evaluate(o: AnyRef): AnyRef =
    Long.box(getExpression(0).evaluate(o).asInstanceOf[java.util.Date].getTime)
}

object DateToLong {
  val Name = new FunctionNameImpl(
    "dateToLong",
    classOf[java.lang.Long],
    parameter("dtg", classOf[java.util.Date])
  )
}
