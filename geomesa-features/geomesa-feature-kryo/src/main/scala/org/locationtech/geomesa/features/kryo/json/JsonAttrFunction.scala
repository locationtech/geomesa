package org.locationtech.geomesa.features.kryo.json

import org.geotools.filter.{AttributeExpressionImpl, FunctionExpressionImpl}
import org.geotools.filter.capability.FunctionNameImpl
import org.geotools.filter.capability.FunctionNameImpl.parameter
import org.opengis.filter.expression.PropertyName

class JsonAttrFunction extends FunctionExpressionImpl(
  new FunctionNameImpl("jsonattr",
    parameter("propertyName", classOf[PropertyName]),
    parameter("string", classOf[String]))
  ) {
  // TODO extract jsonpath
  override def evaluate(`object`: scala.Any): AnyRef = new AttributeExpressionImpl("$.json.[foo path]")
}
