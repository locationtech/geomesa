/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
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
import org.geotools.filter.expression.PropertyAccessor
import org.locationtech.geomesa.utils.geotools.filter.FilterFunctions
import org.locationtech.geomesa.utils.geotools.{SimpleFeaturePropertyAccessor, SimpleFeatureTypes}
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.expression.{PropertyName, VolatileFunction}

class JsonPathFilterFunction extends FunctionExpressionImpl(
  new FunctionNameImpl("jsonPath",
    FilterFunctions.parameter[String]("value"),
    FilterFunctions.parameter[String]("path"), // can be a full path expression, OR an attribute name
    FilterFunctions.parameter[String]("nested-path", required = false)) // (optional) if path is an attribute name, the nested path into the attribute
  ) with LazyLogging with VolatileFunction {

  private val cache = new ConcurrentHashMap[String, PropertyAccessor]

  override def evaluate(obj: java.lang.Object): AnyRef = {
    val sf = try {
      obj.asInstanceOf[SimpleFeature]
    } catch {
      case e: Exception => throw new IllegalArgumentException(s"Expected SimpleFeature, Received ${obj.getClass}. " +
        s"Only simple features are supported. ${obj.toString}", e)
    }
    val base = params.get(0) match {
      case p: PropertyName => p.getPropertyName // for property name expressions, we want the attribute name
      case p => p.evaluate(sf).asInstanceOf[String] // for literals, we want to evaluate the expression
    }
    val path = if (params.size() < 2) { base } else { s"$$.$base.${params.get(1).evaluate(sf)}" }
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
