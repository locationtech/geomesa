package org.locationtech.geomesa.filter.function

import org.geotools.filter.FunctionExpressionImpl
import org.geotools.filter.capability.FunctionNameImpl
import org.joda.time.{DateTime, DateTimeZone}
import org.opengis.feature.simple.SimpleFeature

class CurrentDateFunction
  extends FunctionExpressionImpl(
    new FunctionNameImpl("currentDate", classOf[java.util.Date])
  ) {

  override def evaluate(feature: SimpleFeature): AnyRef = super.evaluate(feature)

  override def evaluate(o: java.lang.Object): AnyRef =
    new DateTime().withZone(DateTimeZone.UTC).toDate

}
