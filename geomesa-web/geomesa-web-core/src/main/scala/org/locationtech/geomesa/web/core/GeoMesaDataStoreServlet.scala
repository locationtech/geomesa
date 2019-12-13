/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.web.core

import java.util.concurrent.ConcurrentHashMap

import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.{DataStore, DataStoreFinder}
import org.scalatra.{BadRequest, NotFound, Ok}

import scala.collection.JavaConversions._
import scala.util.Try

trait GeoMesaDataStoreServlet extends PersistentDataStoreServlet {

  import scala.collection.JavaConverters._

  type PasswordHandler = AnyRef { def encode(value: String): String; def decode(value: String): String }

  private var passwordHandler: Option[PasswordHandler] = None

  private val dataStoreCache = new ConcurrentHashMap[String, DataStore]

  private val passwordKeys =
    DataStoreFinder.getAllDataStores.asScala.flatMap(_.getParametersInfo).collect {
      case p: Param if p.isPassword => p.key
    }.toSet

  sys.addShutdownHook {
    import scala.collection.JavaConversions._
    dataStoreCache.values.foreach(_.dispose())
  }

  protected def withDataStore[D <: DataStore, T](method: (D) => T): Any = {
    val dsParams = datastoreParams
    val key = dsParams.toSeq.sorted.mkString(",")
    val ds = Option(dataStoreCache.get(key).asInstanceOf[D]).getOrElse {
      val ds = DataStoreFinder.getDataStore(dsParams).asInstanceOf[D]
      if (ds != null) {
        dataStoreCache.put(key, ds)
      }
      ds
    }
    if (ds == null) {
      BadRequest(body = "Could not load data store using the provided parameters")
    } else {
      method(ds)
    }
  }

  override def getPersistedDataStore(alias: String): Map[String, String] = {
    val params = super.getPersistedDataStore(alias)
    // noinspection LanguageFeature
    passwordHandler match {
      case None => params
      case Some(handler) =>
        params.map { case (k, v) => (k, if (passwordKeys.contains(k)) { handler.decode(v) } else { v }) }
    }
  }

  /**
   * Registers a data store, making it available for later use
   */
  post("/ds/:alias") {
    val dsParams = datastoreParams
    val ds = Try(DataStoreFinder.getDataStore(dsParams)).getOrElse(null)
    if (ds == null) {
      BadRequest(body = "Could not load data store using the provided parameters.")
    } else {
      dataStoreCache.put(dsParams.toSeq.sorted.mkString(","), ds)
      val alias = params("alias")
      val prefix = keyFor(alias)
      val toPersist = dsParams.map { case (k, v) =>
        // noinspection LanguageFeature
        val value = passwordHandler match {
          case Some(handler) if passwordKeys.contains(k) => handler.encode(v)
          case _ => v
        }
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
    case (k, _) if passwordKeys.contains(k) => (k, "***")
    case (k, v) => (k, v)
  }

  // spring bean accessors for password handler
  def setPasswordHandler(handler: PasswordHandler): Unit = this.passwordHandler = Option(handler)
  def getPasswordHandler: PasswordHandler = passwordHandler.orNull
}

