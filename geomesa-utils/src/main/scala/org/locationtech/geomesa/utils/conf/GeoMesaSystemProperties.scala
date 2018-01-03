/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.conf

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.utils.text.Suffixes

import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

object GeoMesaSystemProperties extends LazyLogging {

  case class SystemProperty(property: String, default: String = null) {

    val threadLocalValue = new ThreadLocal[String]()

    def get: String = ConfigLoader.Config.get(property) match {
      case Some((value, true))  => value // final value - can't be overridden
      case Some((value, false)) => fromSysProps.getOrElse(value)
      case None => fromSysProps.getOrElse(default)
    }

    def option: Option[String] = Option(get)

    def toDuration: Option[Duration] = option.flatMap { value =>
      Try(Duration.apply(value)) match {
        case Success(v) => Some(v)
        case Failure(e) =>
          logger.warn(s"Invalid duration for property $property: $value")
          Option(default).map(Duration.apply)
      }
    }

    def toBytes: Option[Long] = option.flatMap { value =>
      val bytes = Suffixes.Memory.bytes(value)
      if (bytes.nonEmpty) { bytes } else {
        logger.warn(s"Invalid duration for property $property: $value")
        Option(default).flatMap(Suffixes.Memory.bytes)
      }
    }

    def toBoolean: Option[java.lang.Boolean] = option.flatMap { value =>
      Try(Boolean.box(value.toBoolean)) match {
        case Success(v) => Some(v)
        case Failure(e) =>
          logger.warn(s"Invalid Boolean for property $property: $value")
          Option(default).map(java.lang.Boolean.valueOf)
      }
    }

    private def fromSysProps: Option[String] =
      Option(threadLocalValue.get).orElse(sys.props.get(property).filterNot(_.isEmpty))
  }

  // For dynamic properties that are not in geomesa-site.xml.template, this is intended
  // to be a System.getProperty drop-in replacement that ensures the config is always loaded.
  def getProperty(prop: String): String = SystemProperty(prop).get
}

