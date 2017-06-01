/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.conf

object GeoMesaSystemProperties {

  case class SystemProperty(property: String, default: String = null) {
    def get: String = Option(threadLocalValue.get).getOrElse {
      ConfigLoader.Config.get(property) match {
        case Some((value, true))  => value // final value - can't be overridden
        case Some((value, false)) => sys.props.get(property).getOrElse(value)
        case None => sys.props.get(property).filter(_.nonEmpty).getOrElse(default)
      }
    }
    def option: Option[String] = Option(get)

    val threadLocalValue = new ThreadLocal[String]()
  }

  // For dynamic properties that are not in geomesa-site.xml.template, this is intended
  // to be a System.getProperty drop-in replacement that ensures the config is always loaded.
  def getProperty(prop: String): String = SystemProperty(prop).get
}

