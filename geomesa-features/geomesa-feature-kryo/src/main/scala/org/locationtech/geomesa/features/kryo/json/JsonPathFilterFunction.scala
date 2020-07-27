/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.kryo.json

import java.util.concurrent.ConcurrentHashMap

import com.typesafe.scalalogging.LazyLogging
import org.geotools.filter.FunctionExpressionImpl
import org.geotools.filter.capability.FunctionNameImpl
import org.geotools.filter.capability.FunctionNameImpl.parameter
import org.geotools.filter.expression.PropertyAccessor
import org.locationtech.geomesa.utils.geotools.{SimpleFeaturePropertyAccessor, SimpleFeatureTypes}
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.expression.VolatileFunction

class JsonPathFilterFunction extends FunctionExpressionImpl(
  new FunctionNameImpl("jsonPath",
    parameter("value", classOf[String]),
    parameter("path", classOf[String]))
  ) with LazyLogging with VolatileFunction {

  private val cache = new ConcurrentHashMap[String, PropertyAccessor]

  override def evaluate(obj: java.lang.Object): AnyRef = {
    val sf = try {
      obj.asInstanceOf[SimpleFeature]
    } catch {
      case e: Exception => throw new IllegalArgumentException(s"Expected SimpleFeature, Received ${obj.getClass}. " +
        s"Only simple features are supported. ${obj.toString}", e)
    }
    val path = getExpression(0).evaluate(null).asInstanceOf[String]
    var accessor = cache.get(path)
    if (accessor == null) {
      accessor = SimpleFeaturePropertyAccessor.getAccessor(sf, path).getOrElse {
        throw new RuntimeException(s"Can't handle property '$path' for feature type " +
            s"${sf.getFeatureType.getTypeName} ${SimpleFeatureTypes.encodeType(sf.getFeatureType)}")
      }
      cache.put(path, accessor)
    }
    accessor.get(sf, path, classOf[AnyRef])
  }
}
