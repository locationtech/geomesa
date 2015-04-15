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

  override def evaluate(feature: SimpleFeature): jl.Object =
    jl.Long.valueOf(getExpression(0).evaluate(feature).asInstanceOf[java.util.Date].getTime)

  override def evaluate(o: jl.Object): jl.Object =
    jl.Long.valueOf(getExpression(0).evaluate(o).asInstanceOf[java.util.Date].getTime)

}
