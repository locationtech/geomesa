/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.filter.function

import java.time.{Duration, ZoneOffset, ZonedDateTime}
import java.util.Date
import java.util.concurrent.ConcurrentHashMap

import org.geotools.data.Parameter
import org.geotools.filter.FunctionExpressionImpl
import org.geotools.filter.capability.FunctionNameImpl

class CurrentDateFunction extends FunctionExpressionImpl(CurrentDateFunction.Name) {

  private val offsets = new ConcurrentHashMap[String, Duration]()

  override def evaluate(o: AnyRef): AnyRef = {
    val now = ZonedDateTime.now(ZoneOffset.UTC).toInstant
    if (getParameters.isEmpty) {
      Date.from(now)
    } else {
      val offset = getExpression(0).evaluate(null).asInstanceOf[String]
      var duration = offsets.get(offset)
      if (duration == null) {
        duration = Duration.parse(offset)
        offsets.put(offset, duration)
      }
      Date.from(now.plus(duration))
    }
  }
}

object CurrentDateFunction {
  val Name = new FunctionNameImpl("currentDate", classOf[java.util.Date],
    new Parameter[java.lang.String]("offset", classOf[java.lang.String], null, null, false, 0, 1, "P1D", null))
}
