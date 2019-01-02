/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.raster.wcs

case class AccumuloUrl(user: String, password: String, instanceId: String, zookeepers: String,
                       table: String, auths: Option[String] = None, visibilities: Option[String] = None,
                       collectStats: Boolean = false) {

  lazy val url = AccumuloUrl.toUrl(this)

  override def toString: String =
    s"AccumuloUrl($user, ***, $instanceId, $zookeepers, $table, $auths, $visibilities, $collectStats)"
}

object AccumuloUrl {

  private val Regex = """^accumulo://(.*):(.*)@(.*)/(.*)\?zookeepers=([^&]*)(?:&auths=)?([^&]*)""" +
    """(?:&visibilities=)?([^&]*)(?:&collectStats=)?([^&]*)$"""

  private val ParsedUrl = Regex.r

  def apply(url: String): AccumuloUrl = {
    val ParsedUrl(user, password, instanceId, table, zookeepers, auths, visibilities, collectStats) = url
    AccumuloUrl(user, password, instanceId, zookeepers, table, toOption(auths), toOption(visibilities),
      java.lang.Boolean.valueOf(collectStats))
  }

  def toUrl(url: AccumuloUrl): String = {
    StringBuilder.newBuilder
        .append("accumulo://").append(url.user)
        .append(':').append(url.password)
        .append('@').append(url.instanceId)
        .append('/').append(url.table)
        .append("?zookeepers=").append(url.zookeepers)
        .append("&auths=").append(url.auths.getOrElse(""))
        .append("&visibilities=").append(url.visibilities.getOrElse(""))
        .append("&collectStats=").append(url.collectStats)
        .toString()
  }

  private def toOption(s: String) = Option(s).filterNot(_.isEmpty)
}

