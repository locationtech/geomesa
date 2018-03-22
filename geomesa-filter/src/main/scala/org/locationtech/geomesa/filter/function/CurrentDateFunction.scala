/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.filter.function

import java.time.{Duration, ZoneOffset, ZonedDateTime}
import java.util.Date

import org.geotools.data.Parameter
import org.geotools.filter.FunctionExpressionImpl
import org.geotools.filter.capability.FunctionNameImpl

class CurrentDateFunction extends FunctionExpressionImpl(CurrentDateFunction.Name) {
  private var offset = ""

  override def evaluate(o: AnyRef): AnyRef = {
    if (offset.isEmpty && getParameters.size > 0) {
      offset = getExpression(0).evaluate(null).asInstanceOf[String]
    }

    val now = ZonedDateTime.now(ZoneOffset.UTC).toInstant

    if (offset.isEmpty) {
      Date.from(now)
    } else {
      Date.from(now.minus(Duration.parse(offset)))
    }
  }
}

object CurrentDateFunction {
  val param = new Parameter[java.lang.String]("key", classOf[java.lang.String], null, null, false, 0, 1, "P1D", null)

  val Name = new FunctionNameImpl("currentDate", classOf[java.util.Date], param)
}
