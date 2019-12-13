/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.conf

import java.io.InputStream

import com.typesafe.scalalogging.LazyLogging

import scala.util.control.NonFatal
import scala.util.{Failure, Try}
import scala.xml.XML

object ConfigLoader extends LazyLogging {

  val GEOMESA_CONFIG_FILE_PROP = "geomesa.config.file"
  val GEOMESA_CONFIG_FILE_NAME = "geomesa-site.xml"
  private val EmbeddedConfigFile = "org/locationtech/geomesa/geomesa-site.xml.template"

  lazy val Config: Map[String, (String, Boolean)] = {
    val file = Option(System.getProperty(GEOMESA_CONFIG_FILE_PROP)).getOrElse(GEOMESA_CONFIG_FILE_NAME)
    // load defaults first then overwrite with user values (if any)
    loadConfig(EmbeddedConfigFile) ++ loadConfig(file)
  }

  def loadConfig(path: String): Map[String, (String, Boolean)] = {
    val input = getClass.getClassLoader.getResourceAsStream(path)
    val config: Map[String, (String, Boolean)] =
      if (input == null) {
        Map.empty
      } else {
        try {
          logger.debug(s"Loading config: $path")
          loadConfig(input, path)
        } catch {
          case NonFatal(e) =>
            logger.warn(s"Error reading config file at: $path", e)
            Map.empty
        }
      }
    logger.trace(s"Loaded ${config.mkString(",")}")
    config
  }

  def loadConfig(input: InputStream, path: String): Map[String, (String, Boolean)] = {
    val xml = XML.load(input)
    val properties = xml \\ "configuration" \\ "property"
    properties.flatMap { prop =>
      // Use try here so if we fail on a property the rest can still load
      val pair = Try {
        val key = (prop \ "name").text
        val value = (prop \ "value").text
        // don't overwrite properties, this gives commandline params preference
        val isFinal: Boolean = (prop \ "final").text.toString.toBoolean
        key -> (value, isFinal)
      }
      pair match {
        case Failure(e) => logger.warn(s"Unable to load property from: $path\n$prop", e)
        case _ => // no-op
      }
      pair.toOption.filter { case (_, (v, _)) => v != null && v.nonEmpty }
    }.toMap
  }
}
