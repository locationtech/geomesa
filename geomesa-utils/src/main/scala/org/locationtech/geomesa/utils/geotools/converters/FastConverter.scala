/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
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

import scala.reflect.ClassTag
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
    val converters = getConverters(clas, binding)

    var i = 0
    while (i < converters.length) {
      try {
        val result = converters(i).convert(value, binding)
        if (result != null) {
          return result
        }
      } catch {
        case NonFatal(e) =>
          logger.trace(s"Error converting $value (of type ${value.getClass.getName}) " +
              s"to ${binding.getName} using converter ${converters(i).getClass.getName}:", e)
      }
      i += 1
    }

    logger.warn(s"Could not convert '$value' (of type ${value.getClass.getName}) to ${binding.getName}")

    null.asInstanceOf[T]
  }


  /**
   * Convert the value into one of the given type
   *
   * @param value value to convert
   * @param bindings type to convert to, in order of preference
   * @tparam T type binding
   * @return converted value, or null if it could not be converted
   */
  def convertFirst[T](value: Any, bindings: Iterator[Class[_ <: T]]): T = {
    if (value == null) {
      return null.asInstanceOf[T]
    }

    val clas = value.getClass

    while (bindings.hasNext) {
      val binding = bindings.next
      val converters = getConverters(clas, binding)

      var i = 0
      while (i < converters.length) {
        try {
          val result = converters(i).convert(value, binding)
          if (result != null) {
            return result
          }
        } catch {
          case NonFatal(e) =>
            logger.trace(s"Error converting $value (of type ${value.getClass.getName}) " +
                s"to ${binding.getName} using converter ${converters(i).getClass.getName}:", e)
        }
        i += 1
      }
    }

    logger.warn(
      s"Could not convert '$value' (of type ${value.getClass.getName}) " +
          s"to any of ${bindings.map(_.getName).mkString(", ")}")

    null.asInstanceOf[T]
  }

  /**
    * Convert the value into the given type, returning the default if it could not be converted or is null
    *
    * @param value value to convert
    * @param default value to return if convert results in null
    * @param ct class tag
    * @tparam T type to convert to
    * @return
    */
  def convertOrElse[T <: AnyRef](value: Any, default: => T)(implicit ct: ClassTag[T]): T = {
    val attempt = convert(value, ct.runtimeClass.asInstanceOf[Class[T]])
    if (attempt == null) { default } else { attempt }
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

  /**
   * Gets a cached converter, loading it if necessary
   *
   * @param from from
   * @param to to
   * @return
   */
  private def getConverters(from: Class[_], to: Class[_]): Array[Converter] = {
    var converters = cache.get((from, to))

    if (converters == null) {
      if (from.eq(to) || from == to || to.isAssignableFrom(from)) {
        converters = Array(IdentityConverter)
      } else {
        converters = factories.flatMap(factory => Option(factory.createConverter(from, to, null)))
        if (to == classOf[String]) {
          converters = converters :+ ToStringConverter // add toString as a final fallback
        }
      }
      cache.put((from, to), converters)
    }

    converters
  }

  private object IdentityConverter extends Converter {
    override def convert[T](source: Any, target: Class[T]): T = source.asInstanceOf[T]
  }

  private object ToStringConverter extends Converter {
    override def convert[T](source: Any, target: Class[T]): T = source.toString.asInstanceOf[T]
  }
}
