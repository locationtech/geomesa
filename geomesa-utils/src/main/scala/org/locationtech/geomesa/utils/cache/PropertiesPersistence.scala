/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.cache

import java.util.Properties

/**
  * Trait for persisting properties to some kind of durable storage
  */
trait PropertiesPersistence {

  import scala.collection.JavaConversions._

  // make lazy so that load() in subclasses isn't invoked before class is fully constructed
  private lazy val properties = {
    val props = new Properties
    load(props)
    props
  }

  /**
    * Persists the props to storage. Called every time a property is updated or removed
    *
    * @param properties props
    */
  protected def persist(properties: Properties): Unit

  /**
    * Loads properties from storage. Only called once on initialization of class
    *
    * @param properties props
    */
  protected def load(properties: Properties): Unit

  /**
    * Keys in the properties
    *
    * @return
    */
  def keys(): Set[String] = properties.keySet().toSet.asInstanceOf[Set[String]]

  /**
    * Keys that match a prefix
    *
    * @param prefix prefix to match
    * @return
    */
  def keys(prefix: String): Set[String] = keys().filter(_.startsWith(prefix))

  /**
    * All properties
    *
    * @return
    */
  def entries(): Set[(String, String)] =
    properties.entrySet().map(e => (e.getKey, e.getValue)).toSet.asInstanceOf[Set[(String, String)]]

  /**
    * All properties whose keys match a prefix
    *
    * @param prefix prefix to match
    * @return
    */
  def entries(prefix: String): Set[(String, String)] = entries().filter(_._1.startsWith(prefix))

  /**
    * Returns the specified property, if it exists
    *
    * @param key key
    * @return
    */
  def read(key: String): Option[String] = Option(properties.getProperty(key))

  /**
    * Stores the specified property. If calling multiple times, prefer @see persistAll
    *
    * @param key key
    * @param value value - null values will effectively remove the key
    */
  def persist(key: String, value: String): Unit = {
    putOrRemove(key, value)
    persist(properties)
  }

  /**
    * Stores multiple properties at once.
    *
    * @param entries key/value pairs - null values will effectively remove that key
    */
  def persistAll(entries: Map[String, String]): Unit = {
    entries.foreach { case (k, v) => putOrRemove(k, v) }
    persist(properties)
  }

  /**
    * Deletes the key/value pair. If calling multiple times, prefer @see removeAll
    *
    * @param key key
    * @return true if property existed
    */
  def remove(key: String): Boolean = {
    val result = properties.remove(key) != null
    if (result) {
      persist(properties)
    }
    result
  }

  /**
    * Remove multiple keys
    *
    * @param keys keys
    */
  def removeAll(keys: Seq[String]): Unit = {
    var removed = false
    keys.foreach { k => removed = properties.remove(k) != null || removed }
    if (removed) {
      persist(properties)
    }
  }

  private def putOrRemove(key: String, value: String): Unit =
    if (value == null) { properties.remove(key) } else { properties.setProperty(key, value) }
}
