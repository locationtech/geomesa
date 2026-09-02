/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect.auths

import org.geotools.data.jdbc.datasource.ManageableDataSource
import org.locationtech.geomesa.security.AuthorizationsProvider
import org.locationtech.geomesa.utils.io.WithClose

import java.io.PrintWriter
import java.sql.Connection
import java.util.logging.Logger
import javax.sql.DataSource

/**
 * DataSource wrapper that puts the current user's authorizations into a Postgres
 * session variable (GUC) every time a connection is borrowed, so that row-level
 * security policies keyed off the variable (e.g. `current_setting('geomesa.auths', true)`)
 * filter rows correctly.
 *
 * The auths are read from the AuthorizationProvider, and the variable is set
 * unconditionally on every borrow, so a pooled connection can never leak a previous
 * user's auths (a thread with no user sets it to the empty string). Postgres-family
 * databases only.
 *
 * @param delegate the underlying data source (usually the store's pooled data source)
 * @param provider authorizations provider
 */
class SessionDataSource(delegate: DataSource, provider: AuthorizationsProvider) extends ManageableDataSource {

  import scala.collection.JavaConverters._

  override def getConnection: Connection = prepare(delegate.getConnection)

  override def getConnection(username: String, password: String): Connection =
    prepare(delegate.getConnection(username, password))

  private def prepare(cx: Connection): Connection = {
    try {
      val auths = provider.getAuthorizations.asScala.sorted.mkString(",")
      // current_setting('geomesa.auths', true)
      WithClose(cx.prepareStatement(s"SELECT set_config('${SessionDataSource.AuthConfigName}', ?, false)")) { st =>
        st.setString(1, auths)
        st.executeQuery().close()
      }
      cx
    } catch {
      case e: Throwable =>
        try { cx.close() } catch { case suppressed: Throwable => e.addSuppressed(suppressed) }
        throw e
    }
  }

  override def close(): Unit = {
    delegate match {
      case c: AutoCloseable => c.close()
      case _ => // no-op
    }
  }

  override def getLogWriter: PrintWriter = delegate.getLogWriter
  override def setLogWriter(out: PrintWriter): Unit = delegate.setLogWriter(out)
  override def getLoginTimeout: Int = delegate.getLoginTimeout
  override def setLoginTimeout(seconds: Int): Unit = delegate.setLoginTimeout(seconds)
  override def getParentLogger: Logger = delegate.getParentLogger

  // drill into the delegate first (matching AbstractDecorator semantics), so that unwrapping to a general
  // interface like DataSource returns the underlying (pooled/metrics) source rather than this auth wrapper
  override def unwrap[T](iface: Class[T]): T = {
    if (delegate.isWrapperFor(iface)) {
      delegate.unwrap(iface)
    } else if (iface.isInstance(delegate)) {
      iface.cast(delegate)
    } else if (iface.isInstance(this)) {
      iface.cast(this)
    } else {
      delegate.unwrap(iface)
    }
  }
  override def isWrapperFor(iface: Class[_]): Boolean =
    delegate.isWrapperFor(iface) || iface.isInstance(delegate) || iface.isInstance(this)
}

object SessionDataSource {
  val AuthConfigName = "geomesa.auths"
}
