/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.security

/**
 * Delegated identity and authorizations that can be explicitly propagated to executor tasks.
 * Scope complete identities with `withIdentity`, or authorization-only work with `withAuthorizations`.
 * Configured authorization ceilings are still enforced by `AuthUtils.getProvider`.
 */
class DelegatedAuthorizationsProvider extends AuthorizationsProvider {
  override def getAuthorizations: java.util.List[String] = DelegatedAuthorizationsProvider.getAuthorizations
  override def configure(params: java.util.Map[String, _]): Unit = {}
}

object DelegatedAuthorizationsProvider {

  import scala.collection.JavaConverters._

  private val context = new ThreadLocal[DelegatedIdentity]()

  /** An absent identity is distinct from a user with empty groups and auths. */
  def identity: java.util.Optional[DelegatedIdentity] = java.util.Optional.ofNullable(context.get())

  /** Install a complete identity and its auths together; nested authorization scopes may narrow them. */
  def withIdentity[T](identity: DelegatedIdentity)(f: => T): T = {
    java.util.Objects.requireNonNull(identity, "Delegated identity is required")
    withContext(identity)(withAuthorizations(identity.auths().asScala.toSeq.sorted.asJava)(f))
  }

  private def withContext[T](identity: DelegatedIdentity)(f: => T): T = {
    val previous = context.get()
    try {
      if (identity == null) { context.remove() } else { context.set(identity) }
      f
    } finally {
      if (previous == null) { context.remove() } else { context.set(previous) }
    }
  }

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
    val identity = context.get()
    () => withContext(identity)(withAuthorizations(snapshot)(task.run()))
  }
}
