/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.security

import org.apache.accumulo.access.{AccessEvaluator, Authorizations}
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
  def visible(provider: Option[AuthorizationsProvider]): IsVisible = {
    provider match {
      case None    => noAuthVisibilityCheck
      case Some(p) => new AuthVisibilityCheck(p.getAuthorizations)
    }
  }

  /**
   * Used when we don't have an auth provider - any visibilities in the feature will
   * cause the check to fail, so we can skip parsing
   *
   * @param f simple feature to check
   * @return true if feature is visible without any authorizations, otherwise false
   */
  private def noAuthVisibilityCheck(f: SimpleFeature): Boolean = {
    val vis = SecurityUtils.getVisibility(f)
    vis == null || vis.isEmpty
  }

  /**
   * Parses any visibilities in the feature and compares with the user's authorizations
   *
   * @param auths authorizations for the current user
   */
  private class AuthVisibilityCheck(auths: java.util.List[String]) extends (SimpleFeature => Boolean) {

    private val access = AccessEvaluator.of(Authorizations.of(auths))
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
