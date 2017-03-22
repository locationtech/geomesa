/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.filter.function

import org.geotools.filter.FunctionExpressionImpl
import org.geotools.filter.capability.FunctionNameImpl
import org.joda.time.{DateTime, DateTimeZone}
import org.opengis.feature.simple.SimpleFeature

class CurrentDateFunction
  extends FunctionExpressionImpl(
    new FunctionNameImpl("currentDate", classOf[java.util.Date])
  ) {

  def evaluate(feature: SimpleFeature): AnyRef = super.evaluate(feature)

  override def evaluate(o: java.lang.Object): AnyRef =
    new DateTime().withZone(DateTimeZone.UTC).toDate

}
