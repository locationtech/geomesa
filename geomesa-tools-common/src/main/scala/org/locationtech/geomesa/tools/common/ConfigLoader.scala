/***********************************************************************
  * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0
  * which accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  *************************************************************************/

package org.locationtech.geomesa.tools.common

import java.io.FileNotFoundException

import scala.xml.XML
import com.typesafe.scalalogging.LazyLogging

object ConfigLoader extends LazyLogging {

  def findConfig(envHome: String): String = {
    Option(System.getProperty("geomesa.config.file"))
      .getOrElse(s"${System.getenv(envHome)}/conf/geomesa-site.xml")
  }

  def loadConfig(path: String): Unit = {
    try {
      val xml = XML.loadFile(path)
      val properties = xml \ "property"
      properties.foreach { prop =>
        val key = prop \ "name"
        val value = prop \ "value"
        // Don't overwrite properties, this gives commandline flags preference
        if(System.getProperty(key.text).isEmpty){
          System.setProperty(key.text, value.text)
          logger.debug("Set System Property: " + key.text + ":" + value.text)
        }
      }
    } catch {
      case fnfe: FileNotFoundException => logger.warn("Unable to find XML config file at: " + path, fnfe)
      case _: Throwable => logger.warn("Error reading config file: " + path, _)
    }
  }
}