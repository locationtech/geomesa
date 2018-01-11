/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.web.core

import java.util.Locale
import java.util.concurrent.ConcurrentHashMap

import org.geotools.data.DataStoreFinder
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloDataStoreParams}
import org.scalatra.{BadRequest, NotFound, Ok}

import scala.collection.JavaConversions._
import scala.util.Try

trait GeoMesaDataStoreServlet extends PersistentDataStoreServlet {

  import AccumuloDataStoreParams.PasswordParam

  type PasswordHandler = AnyRef { def encode(value: String): String; def decode(value: String): String }

  private var passwordHandler: Option[PasswordHandler] = None

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
    val withPassword = for { handler <- passwordHandler; pw <- Try(PasswordParam.lookupOpt(map)).getOrElse(None) } yield {
      // noinspection LanguageFeature
      map.updated(PasswordParam.getName, handler.decode(pw))
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
        // noinspection LanguageFeature
        val value = if (k == PasswordParam.getName) { passwordHandler.map(_.encode(v)).getOrElse(v) } else { v }
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
      val ds = getPersistedDataStore(params("alias"))
      if (ds.isEmpty) { NotFound() } else {
        filterPasswords(ds)
      }
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
      getPersistedDataStores.mapValues(filterPasswords)
    } catch {
      case e: Exception => handleError(s"Error reading data stores:", e)
    }
  }

  private def filterPasswords(map: Map[String, String]): Map[String, String] = map.map {
    case (k, _) if k.toLowerCase(Locale.US).contains("password") => (k, "***")
    case (k, v) => (k, v)
  }

  // spring bean accessors for password handler
  def setPasswordHandler(handler: PasswordHandler): Unit = this.passwordHandler = Option(handler)
  def getPasswordHandler: PasswordHandler = passwordHandler.orNull
}

