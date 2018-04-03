/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.conf

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

object ConfigManager extends LazyLogging {

  val GEOMESA_CONFIG_FILE_PROP = "geomesa.config.file"
  val GEOMESA_CONFIG_FILE_NAME = "geomesa-site.xml"
  private val EmbeddedConfigFile = "org/locationtech/geomesa/geomesa-site.xml.template"

  lazy val GeoMesaConfig: Configuration = {
    new Configuration()
    val file = Option(System.getProperty(GEOMESA_CONFIG_FILE_PROP)).getOrElse(GEOMESA_CONFIG_FILE_NAME)
    // load defaults first then overwrite with user values (if any)
    loadConfig(EmbeddedConfigFile)
    loadConfig(file)
    GeoMesaConfig
  }

  def getProperty(name: String): (String, Boolean) = {
    val finals = GeoMesaConfig.getFinalParameters
    (GeoMesaConfig.get(name), finals.contains(name))
  }

  def loadConfig(path: String): Unit = {
    GeoMesaConfig.addResource(path)
    logger.trace(s"Loaded ${GeoMesaConfig.toString}")
  }

  def loadConfig(config: Configuration): Unit = {
    GeoMesaConfig.addResource(config)
    logger.trace(s"Loaded ${GeoMesaConfig.toString}")
  }

  /**
   * Augments the GeoMesa configuration with the configuration and/or paths
   * provided. This is used to add other Hadoop configurations to the GeoMesa
   * configuration so it is passed around.
   */
  def augment(base: Configuration): Configuration = {
    loadConfig(base)
    GeoMesaConfig
  }

  def augment(paths: Option[String]): Configuration = {
    val sanitized = paths.filterNot(_.trim.isEmpty).toSeq.flatMap(_.split(',')).map(_.trim).filterNot(_.isEmpty)
    if (sanitized.nonEmpty) {
      sanitized.foreach(p => GeoMesaConfig.addResource(new Path(p)))
    }
    GeoMesaConfig
  }

  def augment(base: Configuration, paths: Option[String]): Configuration = {
    augment(paths)
    augment(base)
  }
}
