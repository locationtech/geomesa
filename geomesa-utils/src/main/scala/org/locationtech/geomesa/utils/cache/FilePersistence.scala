/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.cache

import java.io.{File, FileInputStream, FileOutputStream}
import java.util.Properties

import com.typesafe.scalalogging.LazyLogging

import scala.collection.JavaConversions._

/**
 * Simple persistence strategy that keeps values in memory and writes them to a prop file on disk.
 */
class FilePersistence(dir: File, file: String) extends LazyLogging {

  // ensure directory is present and available
  require((!dir.exists() && dir.mkdirs()) || dir.isDirectory)

  private val properties = new Properties
  private val configFile = new File(dir, file)

  logger.debug(s"Using data file '${configFile.getAbsolutePath}'")

  if (configFile.exists) {
    val inputStream = new FileInputStream(configFile)
    try {
      properties.load(inputStream)
    } finally {
      inputStream.close()
    }
  }

  def keys(): Set[String] = properties.keySet().toSet.asInstanceOf[Set[String]]
  def keys(prefix: String): Set[String] = keys().filter(_.startsWith(prefix))

  def entries(): Set[(String, String)] =
    properties.entrySet().map(e => (e.getKey, e.getValue)).toSet.asInstanceOf[Set[(String, String)]]
  def entries(prefix: String): Set[(String, String)] = entries().filter(_._1.startsWith(prefix))

  /**
   * Returns the specified property
   *
   * @param key
   * @return
   */
  def read(key: String): Option[String] = Option(properties.getProperty(key))

  /**
   * Stores the specified property. If calling multiple times, prefer @see persistAll
   *
   * @param key
   * @param value
   */
  def persist(key: String, value: String): Unit = {
    putOrRemove(key, value)
    persist(properties)
  }

  /**
   * Stores multiple properties at once.
   *
   * @param entries
   */
  def persistAll(entries: Map[String, String]): Unit = {
    entries.foreach { case (k, v) => putOrRemove(k, v) }
    persist(properties)
  }

  def remove(key: String): Boolean = {
    val result = properties.remove(key) != null
    if (result) {
      persist(properties)
    }
    result
  }

  def removeAll(keys: Seq[String]): Unit = {
    keys.foreach(properties.remove)
    persist(properties)
  }

  /**
   *
   * @param key
   * @param value
   */
  private def putOrRemove(key: String, value: String): Unit =
    if (value == null) { properties.remove(key) } else { properties.setProperty(key, value) }

  /**
   * Persists the props to a file
   *
   * @param properties
   */
  private def persist(properties: Properties): Unit =
    this.synchronized {
      val outputStream = new FileOutputStream(configFile)
      try {
        properties.store(outputStream, "GeoMesa configuration file")
      } finally {
        outputStream.close()
      }
    }
}
