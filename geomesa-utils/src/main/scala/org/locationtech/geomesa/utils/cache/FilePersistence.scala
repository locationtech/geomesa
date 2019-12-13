/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.cache

import java.io.{File, FileInputStream, FileOutputStream}
import java.util.Properties

import com.typesafe.scalalogging.LazyLogging

/**
 * Simple persistence strategy that keeps values in memory and writes them to a prop file on disk.
 */
class FilePersistence(dir: File, file: String) extends PropertiesPersistence with LazyLogging {

  // ensure directory is present and available
  require((!dir.exists() && dir.mkdirs()) || dir.isDirectory)

  private val configFile = new File(dir, file)

  logger.debug(s"Using data file '${configFile.getAbsolutePath}'")

  override protected def load(properties: Properties): Unit = this.synchronized {
    if (configFile.exists) {
      val inputStream = new FileInputStream(configFile)
      try {
        properties.load(inputStream)
      } finally {
        inputStream.close()
      }
    }
  }

  override protected def persist(properties: Properties): Unit = this.synchronized {
    val outputStream = new FileOutputStream(configFile)
    try {
      properties.store(outputStream, "GeoMesa configuration file")
    } finally {
      outputStream.close()
    }
  }
}
