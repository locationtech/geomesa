/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.security

/**
 * AuthorizationsProvider that wraps another provider and ensures that the auths returned do not exceed a pre-set list
 */
class FilteringAuthorizationsProvider(val wrappedProvider: AuthorizationsProvider, filter: java.util.List[String])
    extends AuthorizationsProvider {

  override def getAuthorizations: java.util.List[String] = {
    val auths = new java.util.ArrayList(wrappedProvider.getAuthorizations)
    auths.retainAll(filter)
    auths
  }

  override def currentUser(): DelegatedIdentity = {
    val user = wrappedProvider.currentUser()
    val auths = new java.util.ArrayList(user.auths())
    auths.retainAll(filter)
    new DelegatedIdentity(user.user(), user.groups(), auths)
  }

  override def configure(params: java.util.Map[String, _]): Unit = wrappedProvider.configure(params)
}
