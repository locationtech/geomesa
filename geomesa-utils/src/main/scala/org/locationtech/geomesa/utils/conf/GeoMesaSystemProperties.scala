/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.conf

import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

object GeoMesaSystemProperties extends LazyLogging {

  case class SystemProperty(property: String, default: String = null) {

    val threadLocalValue = new ThreadLocal[String]()

    def get: String = Option(threadLocalValue.get).getOrElse {
      ConfigLoader.Config.get(property) match {
        case Some((value, true))  => value // final value - can't be overridden
        case Some((value, false)) => sys.props.get(property).getOrElse(value)
        case None => sys.props.get(property).filter(_.nonEmpty).getOrElse(default)
      }
    }

    def option: Option[String] = Option(get)

    def toDuration: Option[Long] = option.flatMap { value =>
      Try(Duration.apply(value).toMillis) match {
        case Success(m) => Some(m)
        case Failure(e) => logger.warn(s"Invalid duration for property $property: $value"); None
      }
    }
  }

  // For dynamic properties that are not in geomesa-site.xml.template, this is intended
  // to be a System.getProperty drop-in replacement that ensures the config is always loaded.
  def getProperty(prop: String): String = SystemProperty(prop).get
}

