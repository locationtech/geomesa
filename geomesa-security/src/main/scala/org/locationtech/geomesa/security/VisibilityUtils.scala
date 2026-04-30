/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.security

import org.apache.accumulo.access.Access
import org.geotools.api.feature.simple.SimpleFeature

import scala.util.control.NonFatal

object VisibilityUtils {

  type IsVisible = SimpleFeature => Boolean

  /**
   * Return a local function that will check features for visibility, based on the user's current authorizations.
   * Not thread-safe or re-usable in a subsequent request.
   *
   * @param provider auth provider
   * @return
   */
  def visible(provider: AuthorizationsProvider): IsVisible =
    new AuthVisibilityCheck(new java.util.HashSet[String](provider.getAuthorizations))

  /**
   * Parses any visibilities in the feature and compares with the user's authorizations
   *
   * @param auths authorizations for the current user
   */
  private class AuthVisibilityCheck(auths: java.util.Set[String]) extends (SimpleFeature => Boolean) {

    private val access = Access.builder().build().newEvaluator(auths)
    private val cache = scala.collection.mutable.Map.empty[String, Boolean]

    /**
     * Checks auths against the feature's visibility
     *
     * @param f feature
     * @return true if feature is visible to the current user, otherwise false
     */
    override def apply(f: SimpleFeature): Boolean = {
      val vis = SecurityUtils.getVisibility(f)
      vis == null || cache.getOrElseUpdate(vis, try { access.canAccess(vis) } catch { case NonFatal(_) => false })
    }
  }
}
