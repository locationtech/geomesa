/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.utils.conf

object GeoMesaSystemProperties {

  val CONFIG_FILE = SystemProperty("geomesa.config.file", "geomesa-site.xml")

  case class SystemProperty(property: String, default: String) {
    def get: String = Option(threadLocalValue.get).getOrElse(sys.props.getOrElse(property, default))
    def option: Option[String] = Option(threadLocalValue.get).orElse(sys.props.get(property)).orElse(Option(default))
    def set(value: String): Unit = sys.props.put(property, value)
    def clear(): Unit = sys.props.remove(property)

    val threadLocalValue = new ThreadLocal[String]()
  }
}

