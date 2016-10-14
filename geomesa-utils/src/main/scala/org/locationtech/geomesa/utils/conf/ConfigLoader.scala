/***********************************************************************
  * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0
  * which accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  *************************************************************************/

package org.locationtech.geomesa.utils.conf

import scala.xml.XML
import java.io.FileNotFoundException

import com.typesafe.scalalogging.LazyLogging

object ConfigLoader extends LazyLogging {
  val ACCUMULO_HOME = "ACCUMULO_HOME"
  val KAFKA_HOME = "KAFKA_HOME"
  val CONFIG_PATH = "/conf/geomesa-site.xml"

  def init(): Unit = {
    if (! Option(System.getenv(ACCUMULO_HOME)).getOrElse("").isEmpty) {
      this.init(ACCUMULO_HOME)
    } else if (! Option(System.getenv(KAFKA_HOME)).getOrElse("").isEmpty) {
      this.init(KAFKA_HOME)
    } else {
      logger.warn("Unable to locate GeoMesa config file.")
    }
  }

  def init(envName: String): Unit = {
    if (! Option(System.getProperty("geomesa.config.file")).getOrElse("").isEmpty){
      this.loadConfig(System.getProperty("geomesa.config.file"))
    } else {
      this.loadConfig(s"${System.getenv(envName)}$CONFIG_PATH")
    }
  }

  def loadConfig(path: String): Unit = {
    try {
      val xml = XML.loadFile(path)
      logger.info("Using GeoMesa config file found at: " + path)
      val properties = xml \ "property"
      properties.foreach { prop =>
        val key = prop \ "name"
        val value = prop \ "value"
        val isfinal: Boolean = (prop \ "final").toString().toBoolean
        // Don't overwrite properties, this gives commandline params preference
        if(System.getProperty(key.text).isEmpty && value.nonEmpty || isfinal){
          System.setProperty(key.text, value.text)
          logger.debug("Set System Property: " + key.text + ":" + value.text)
        }
      }
    } catch {
      case fnfe: FileNotFoundException => logger.warn("Unable to find GeoMesa config file at: " + path, fnfe)
      case _: Throwable => logger.warn("Error reading config file: " + path, _: Throwable)
    }
  }
}