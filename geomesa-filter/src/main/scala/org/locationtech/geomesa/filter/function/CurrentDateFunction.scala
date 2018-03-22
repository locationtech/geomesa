/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.filter.function

import java.time.{ZoneOffset, ZonedDateTime}
import java.util.Date

import org.geotools.filter.FunctionExpressionImpl
import org.geotools.filter.capability.FunctionNameImpl

class CurrentDateFunction extends FunctionExpressionImpl(CurrentDateFunction.Name) {
  override def evaluate(o: AnyRef): AnyRef = Date.from(ZonedDateTime.now(ZoneOffset.UTC).toInstant)
}

object CurrentDateFunction {
  val Name = new FunctionNameImpl("currentDate", classOf[java.util.Date])
}
