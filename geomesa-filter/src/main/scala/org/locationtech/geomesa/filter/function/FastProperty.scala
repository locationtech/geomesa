package org.locationtech.geomesa.filter.function

import org.geotools.filter.FunctionExpressionImpl
import org.geotools.filter.capability.FunctionNameImpl
import org.geotools.filter.capability.FunctionNameImpl._
import org.opengis.feature.simple.SimpleFeature

class FastProperty extends FunctionExpressionImpl(
  new FunctionNameImpl("fastproperty",
    parameter("propertyValue", classOf[Object]),
    parameter("propertyIndex", classOf[Integer]))) {

  var idx = -1

  def this(i: Int) = {
    this()
    idx = i
  }

  override def evaluate(o: java.lang.Object): AnyRef = {
    if (idx == -1) {
      idx = getExpression(0).evaluate(null).asInstanceOf[Long].toInt
    }
    o.asInstanceOf[SimpleFeature].getAttribute(idx)
  }
}
