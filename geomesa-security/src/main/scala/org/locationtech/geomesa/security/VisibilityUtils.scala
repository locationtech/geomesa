/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.security

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.access.{AccessEvaluator, Authorizations}
import org.geotools.api.feature.simple.SimpleFeature

import scala.util.control.NonFatal

object VisibilityUtils {

  /**
   * Return a local function that will check features for visibility, based on the user's current authorizations.
   * Not thread-safe or re-usable in a subsequent request.
   *
   * @param provider auth provider
   * @return
   */
  def visible(provider: AuthorizationsProvider): SimpleFeature => Boolean =
    new AuthVisibilityFeatureCheck(provider.getAuthorizations)

  /**
   *
   * @param provider
   * @return
   */
  def check(provider: AuthorizationsProvider): String => Boolean = new AuthVisibilityCheck(provider.getAuthorizations)

  /**
   * Parses any visibilities in the feature and compares with the user's authorizations
   *
   * @param auths authorizations for the current user
   */
  private class AuthVisibilityFeatureCheck(auths: java.util.List[String]) extends (SimpleFeature => Boolean) {

    private val check = new AuthVisibilityCheck(auths)

    /**
     * Checks auths against the feature's visibility
     *
     * @param f feature
     * @return true if feature is visible to the current user, otherwise false
     */
    override def apply(f: SimpleFeature): Boolean = check(SecurityUtils.getVisibility(f))
  }

  /**
   * Parses any visibilities in the feature and compares with the user's authorizations
   *
   * @param auths authorizations for the current user
   */
  private class AuthVisibilityCheck(auths: java.util.List[String]) extends (String => Boolean) with LazyLogging {

    private val access = AccessEvaluator.of(Authorizations.of(auths))
    private val cache = scala.collection.mutable.Map.empty[String, Boolean]

    /**
     * Checks auths against visibility
     *
     * @param vis visibility string
     * @return true if feature is visible to the current user, otherwise false
     */
    override def apply(vis: String): Boolean = vis == null || cache.getOrElseUpdate(vis, canAccess(vis))

    private def canAccess(vis: String): Boolean = {
      try { access.canAccess(vis) } catch {
        case NonFatal(e) =>
          logger.warn("Error evaluating visibility expression:", e)
          false
      }
    }
  }
}
