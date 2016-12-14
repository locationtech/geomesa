/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.web.core

import java.util.concurrent.ConcurrentHashMap

import org.geotools.data.DataStoreFinder
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreParams
import org.scalatra.{BadRequest, Ok}

import scala.collection.JavaConversions._
import scala.util.Try

trait GeoMesaDataStoreServlet extends PersistentDataStoreServlet {

  type PasswordHandler = AnyRef { def encode(value: String): String; def decode(value: String): String }

  private var passwordHandler: PasswordHandler = null
  private val passwordKey = AccumuloDataStoreParams.passwordParam.getName

  private val dataStoreCache = new ConcurrentHashMap[String, AccumuloDataStore]

  sys.addShutdownHook {
    import scala.collection.JavaConversions._
    dataStoreCache.values.foreach(_.dispose())
  }

  protected def withDataStore[T](method: (AccumuloDataStore) => T): Any = {
    val dsParams = datastoreParams
    val key = dsParams.toSeq.sorted.mkString(",")
    val ds = Option(dataStoreCache.get(key)).getOrElse {
      val ds = DataStoreFinder.getDataStore(dsParams).asInstanceOf[AccumuloDataStore]
      if (ds != null) {
        dataStoreCache.put(key, ds)
      }
      ds
    }
    if (ds == null) {
      BadRequest(reason = "Could not load data store using the provided parameters.")
    } else {
      method(ds)
    }
  }

  override def getPersistedDataStore(alias: String): Map[String, String] = {
    val map = super.getPersistedDataStore(alias)
    val withPassword = for { handler <- Option(passwordHandler); pw <- map.get(passwordKey) } yield {
      map.updated(passwordKey, handler.decode(pw))
    }
    withPassword.getOrElse(map)
  }

  /**
   * Registers a data store, making it available for later use
   */
  post("/ds/:alias") {
    val dsParams = datastoreParams
    val ds = Try(DataStoreFinder.getDataStore(dsParams).asInstanceOf[AccumuloDataStore]).getOrElse(null)
    if (ds == null) {
      BadRequest(reason = "Could not load data store using the provided parameters.")
    } else {
      dataStoreCache.put(dsParams.toSeq.sorted.mkString(","), ds)
      val alias = params("alias")
      val prefix = keyFor(alias)
      val toPersist = dsParams.map { case (k, v) =>
        val value = if (k == passwordKey && passwordHandler != null) { passwordHandler.encode(v) } else { v }
        keyFor(alias, k) -> value
      }
      try {
        persistence.removeAll(persistence.keys(prefix).toSeq)
        persistence.persistAll(toPersist)
        Ok()
      } catch {
        case e: Exception => handleError(s"Error persisting data store '$alias':", e)
      }
    }
  }

  /**
   * Retrieve an existing data store
   */
  get("/ds/:alias") {
    try {
      getPersistedDataStore(params("alias"))
    } catch {
      case e: Exception => handleError(s"Error reading data store:", e)
    }
  }

  /**
   * Remove the reference to an existing data store
   */
  delete("/ds/:alias") {
    val alias = params("alias")
    val prefix = keyFor(alias)
    try {
      persistence.removeAll(persistence.keys(prefix).toSeq)
      Ok()
    } catch {
      case e: Exception => handleError(s"Error removing data store '$alias':", e)
    }
  }

  /**
   * Retrieve all existing data stores
   */
  get("/ds/?") {
    try {
      getPersistedDataStores
    } catch {
      case e: Exception => handleError(s"Error reading data stores:", e)
    }
  }

  // spring bean accessors for password handler
  def setPasswordHandler(handler: PasswordHandler): Unit = this.passwordHandler = handler
  def getPasswordHandler: PasswordHandler = passwordHandler
}

