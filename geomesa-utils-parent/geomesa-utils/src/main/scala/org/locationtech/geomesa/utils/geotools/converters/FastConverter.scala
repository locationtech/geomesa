/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools.converters

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
import com.typesafe.scalalogging.StrictLogging
import org.geotools.api.filter.expression.Expression
import org.geotools.data.util.InterpolationConverterFactory
import org.geotools.util.factory.GeoTools
import org.geotools.util.{Converter, Converters}
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty

import java.util.concurrent.TimeUnit
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.control.NonFatal

/**
  * Replacement for geotools `Converters`, which caches the converters for each pair of (from -> to) instead of
  * re-creating them each time.
  */
object FastConverter extends StrictLogging {

  import scala.collection.JavaConverters._

  val ConverterCacheExpiry: SystemProperty = SystemProperty("geomesa.type.converter.cache.expiry", "1 hour")

  private val cache: LoadingCache[(Class[_], Class[_]), Array[Converter]] =
    Caffeine.newBuilder().expireAfterWrite(ConverterCacheExpiry.toDuration.get.toMillis, TimeUnit.MILLISECONDS).build(
      new CacheLoader[(Class[_], Class[_]), Array[Converter]]() {
        override def load(key: (Class[_], Class[_])): Array[Converter] = {
          val (from, to) = key
          val factories = Converters.getConverterFactories(GeoTools.getDefaultHints).asScala.toArray.filter {
            // exclude jai-related factories as it's not usually on the classpath
            case _: InterpolationConverterFactory => false
            case _ => true
          }
          logger.debug(s"Loaded ${factories.length} converter factories: ${factories.map(_.getClass.getName).mkString(", ")}")
          val converters = factories.flatMap(factory => Option(factory.createConverter(from, to, null)))
          logger.debug(
            s"Found ${converters.length} converters for ${from.getName}->${to.getName}: " +
              s"${converters.map(_.getClass.getName).mkString(", ")}")
          if (to == classOf[String]) {
            converters :+ ToStringConverter // add toString as a final fallback
          } else {
            converters
          }
        }
      }
    )

  /**
    * Convert the value into the given type
    *
    * @param value value to convert
    * @param binding type to convert to
    * @tparam T type binding
    * @return converted value, or null if it could not be converted
    */
  def convert[T](value: Any, binding: Class[T]): T = {
    if (value == null || binding.isAssignableFrom(value.getClass)) {
      return value.asInstanceOf[T]
    }

    val converters = cache.get((value.getClass, binding))

    val result = tryConvert(value, binding, converters)
    if (result == null) {
      val msg =
        s"Could not convert '$value' (of type ${value.getClass.getName}) to ${binding.getName} " +
          s"using ${converters.map(_.getClass.getName).mkString(", ")}"
      logger.warn(msg)
      logger.debug(msg, new RuntimeException())
    }
    result
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
    val errors = ArrayBuffer.empty[(Class[_], Array[Converter])]

    while (bindings.hasNext) {
      val binding = bindings.next
      if (binding.isAssignableFrom(clas)) {
        return value.asInstanceOf[T]
      }

      val converters = cache.get((clas, binding))

      val result = tryConvert(value, binding, converters)
      if (result != null) {
        return result
      } else {
        errors += binding -> converters
      }
    }

    val msg =
      s"Could not convert '$value' (of type ${value.getClass.getName}) to any of:" +
        errors.map { case (b, c) => s"${b.getClass.getName} using ${c.map(_.getClass.getName).mkString(", ")}"}.mkString("\n  ", "\n  ", "")

    logger.warn(msg)
    logger.debug(msg, new RuntimeException())

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
   * Try to convert the value
   *
   * @param value value
   * @param binding expected return type
   * @param converters converters to use
   * @tparam T expected return type
   * @return the converted value as type T, or null if could not convert
   */
  private def tryConvert[T](value: Any, binding: Class[T], converters: Array[Converter]): T = {
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
    null.asInstanceOf[T]
  }

  private object ToStringConverter extends Converter {
    override def convert[T](source: Any, target: Class[T]): T = source.toString.asInstanceOf[T]
  }
}
