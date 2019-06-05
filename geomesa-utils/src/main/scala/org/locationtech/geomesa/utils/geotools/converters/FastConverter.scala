/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools.converters

import java.util.concurrent.ConcurrentHashMap

import com.typesafe.scalalogging.StrictLogging
import org.geotools.data.util.InterpolationConverterFactory
import org.geotools.util.factory.GeoTools
import org.geotools.util.{Converter, Converters}
import org.opengis.filter.expression.Expression

import scala.util.control.NonFatal

/**
  * Replacement for geotools `Converters`, which caches the converters for each pair of (from -> to) instead of
  * re-creating them each time.
  */
object FastConverter extends StrictLogging {

  import scala.collection.JavaConverters._

  private val factories = Converters.getConverterFactories(GeoTools.getDefaultHints).asScala.toArray.filter {
    // exclude jai-related factories as it's not usually on the classpath
    case _: InterpolationConverterFactory => false
    case _ => true
  }

  private val cache = new ConcurrentHashMap[(Class[_], Class[_]), Array[Converter]]

  /**
    * Convert the value into the given type
    *
    * @param value value to convert
    * @param binding type to convert to
    * @tparam T type binding
    * @return converted value, or null if it could not be converted
    */
  def convert[T](value: Any, binding: Class[T]): T = {
    if (value == null) {
      return null.asInstanceOf[T]
    }

    val clas = value.getClass
    var converters = cache.get((clas, binding))

    if (converters == null) {
      if (clas.eq(binding) || clas == binding || binding.isAssignableFrom(clas)) {
        converters = Array(IdentityConverter)
      } else {
        converters = factories.flatMap(factory => Option(factory.createConverter(clas, binding, null)))
        if (binding == classOf[String]) {
          converters = converters :+ ToStringConverter // add toString as a final fallback
        }
      }
      cache.put((clas, binding), converters)
    }

    converters.foreach { converter =>
      try {
        val result = converter.convert(value, binding)
        if (result != null) {
          return result
        }
      } catch {
        case NonFatal(e) =>
          logger.trace(s"Error converting $value (of type ${value.getClass.getName}) " +
            s"to ${binding.getName} using converter ${converter.getClass.getName}:", e)
      }
    }

    logger.warn(s"Could not convert '$value' (of type ${value.getClass.getName}) to ${binding.getName}")

    null.asInstanceOf[T]
  }

  /**
    * Evaluate and convert an expression
    *
    * @param expression expression to evaluate
    * @param binding type to convert to
    * @tparam T type binding
    * @return converted value, or null if it could not be converted
    */
  def evaluate[T](expression: Expression, binding: Class[T]): T = convert(expression.evaluate(null), binding)

  private object IdentityConverter extends Converter {
    override def convert[T](source: Any, target: Class[T]): T = source.asInstanceOf[T]
  }

  private object ToStringConverter extends Converter {
    override def convert[T](source: Any, target: Class[T]): T = source.toString.asInstanceOf[T]
  }
}
