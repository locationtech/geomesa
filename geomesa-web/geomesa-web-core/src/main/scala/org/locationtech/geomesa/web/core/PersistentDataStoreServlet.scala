/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.web.core

import org.locationtech.geomesa.utils.cache.PropertiesPersistence

trait PersistentDataStoreServlet extends GeoMesaScalatraServlet {

  def persistence: PropertiesPersistence

  override def datastoreParams: Map[String, String] = {
    val ds = super.datastoreParams
    if (ds.nonEmpty) { ds } else {
      params.get("alias").map(getPersistedDataStore).getOrElse(Map.empty)
    }
  }

  /**
    * Gets a persisted data store config
    */
  def getPersistedDataStore(alias: String): Map[String, String] = {
    val key = keyFor(alias)
    persistence.entries(key).map { case (k, v) => (k.substring(key.length), v) }.toMap
  }

  /**
    * Gets all persisted data stores by alias
    */
  def getPersistedDataStores: Map[String, Map[String, String]] = {
    val aliases = persistence.keys("ds.").map(k => k.substring(3, k.indexOf('.', 3)))
    aliases.map(a => a -> getPersistedDataStore(a)).toMap
  }

  protected def keyFor(alias: String, param: String = "") = s"ds.$alias.$param"

}
