package org.locationtech.geomesa.features.kryo.json

import com.typesafe.scalalogging.LazyLogging
import org.geotools.filter.{AttributeExpressionImpl, FunctionExpressionImpl}
import org.geotools.filter.capability.FunctionNameImpl
import org.geotools.filter.capability.FunctionNameImpl.parameter
import org.geotools.filter.expression.PropertyAccessors
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.expression.PropertyName

class JsonAttrFunction extends FunctionExpressionImpl(
  new FunctionNameImpl("jsonattr",
    parameter("propertyName", classOf[PropertyName]),
    parameter("string", classOf[String]))
  ) with LazyLogging {

  override def evaluate(obj: java.lang.Object): AnyRef = {
    //new AttributeExpressionImpl("$.json.[foo path]")
    val sf = try {
      obj.asInstanceOf[SimpleFeature]
    } catch {
      case e: Exception => throw new IllegalArgumentException("Only simple features are supported", e)
    }
    //val property = getExpression(0).evaluate(null).asInstanceOf[String]
    val path = getExpression(0).evaluate(null).asInstanceOf[String]
    // some mojo to ensure our property accessor is picked up -
    // our accumulo iterators are not generally available in the system classloader
    // instead, we can set the context classloader (as that will be checked if set)
    val contextClassLoader = Thread.currentThread.getContextClassLoader
    if (contextClassLoader != null) {
      logger.warn(s"Bypassing context classloader $contextClassLoader for PropertyAccessor loading")
    }
    Thread.currentThread.setContextClassLoader(classOf[JsonAttrFunction].getClassLoader)
    val accessor = try {
      import scala.collection.JavaConversions._
      PropertyAccessors.findPropertyAccessors(sf, path, null, null).find(_.canHandle(sf, path, classOf[AnyRef]))
    } finally {
      // reset the classloader after loading the accessors
      Thread.currentThread.setContextClassLoader(contextClassLoader)
    }
    accessor match {
      case Some(a) => a.get(sf, path, classOf[AnyRef])
      case None    => throw new RuntimeException(s"Can't handle property '$name' for feature type " +
        s"${sf.getFeatureType.getTypeName} ${SimpleFeatureTypes.encodeType(sf.getFeatureType)}")
    }
  }
}
