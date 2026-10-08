/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.security

/**
 * Request-scoped authorizations that can be explicitly propagated to executor tasks.
 * Applications should extend this class and scope requests with `withAuthorizations`.
 * Configured authorization ceilings are still enforced by `AuthUtils.getProvider`.
 */
class ThreadLocalAuthorizationsProvider extends AuthorizationsProvider {
  override def getAuthorizations: java.util.List[String] = ThreadLocalAuthorizationsProvider.getAuthorizations
  override def configure(params: java.util.Map[String, _]): Unit = {}
}

object ThreadLocalAuthorizationsProvider {

  private val authorizations = new ThreadLocal[java.util.List[String]]()

  private def getAuthorizations: java.util.List[String] =
    Option(authorizations.get()).getOrElse(java.util.Collections.emptyList[String]())

  /** Run with an immutable authorization snapshot, restoring the previous scope even on failure. */
  def withAuthorizations[T](auths: java.util.List[String])(f: => T): T = {
    val snapshot = java.util.List.copyOf(auths)
    val previous = authorizations.get()
    try {
      authorizations.set(snapshot)
      f
    } finally {
      if (previous == null) { authorizations.remove() } else { authorizations.set(previous) }
    }
  }

  /** Capture on the submitting thread, then install and restore the scope on the executor thread. */
  def wrap(task: Runnable): Runnable = {
    val snapshot = getAuthorizations
    () => withAuthorizations(snapshot)(task.run())
  }
}
