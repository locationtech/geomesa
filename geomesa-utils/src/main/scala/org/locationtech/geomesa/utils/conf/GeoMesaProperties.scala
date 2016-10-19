/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.utils.conf

import java.util.Properties

import com.typesafe.scalalogging.LazyLogging

object GeoMesaProperties extends LazyLogging {

  private val EmbeddedFile = "/org/locationtech/geomesa/geomesa.properties"

  private val props: Properties = {
    val resource = getClass.getResourceAsStream(EmbeddedFile)
    val props = new Properties
    if (resource == null) {
      logger.warn(s"Couldn't load $EmbeddedFile")
      props
    } else {
      try {
        props.load(resource)
      } finally {
        resource.close()
      }
      props
    }
  }

  val ProjectVersion = props.getProperty("geomesa.project.version")
  val BuildDate      = props.getProperty("geomesa.build.date")
  val GitCommit      = props.getProperty("geomesa.build.commit.id")
  val GitBranch      = props.getProperty("geomesa.build.branch")
//
  def GEOMESA_TOOLS_ACCUMULO_SITE_XML: String = {
    val siteXML = getProperty("geomesa.tools.accumulo.site.xml")
    if (siteXML.nonEmpty) siteXML
    else s"${System.getenv("ACCUMULO_HOME")}/conf/accumulo-site.xml"
  }
  def GEOMESA_AUDIT_PROVIDER_IMPL       = propOrDefault("geomesa.audit.provider.impl")
  def GEOMESA_AUTH_PROVIDER_IMPL        = propOrDefault("geomesa.auth.provider.impl")
  def GEOMESA_BATCHWRITER_LATENCY_MILLS = propOrDefault("geomesa.batchwriter.latency.millis")
  def GEOMESA_BATCHWRITER_MAXTHREADS    = propOrDefault("geomesa.batchwriter.maxthreads")
  def GEOMESA_BATCHWRITER_MEMORY        = propOrDefault("geomesa.batchwriter.memory")
  def GEOMESA_BATCHWRITER_TIMEOUT_MILLS = propOrDefault("geomesa.batchwriter.timeout.millis")
  def GEOMESA_CONVERT_CONFIG_URLS       = propOrDefault("geomesa.convert.config.urls")
  def GEOMESA_CONVERT_SCRIPTS_PATH      = propOrDefault("geomesa.convert.scripts.path")
  def GEOMESA_FEATURE_ID_GENERATOR      = propOrDefault("geomesa.feature.id-generator")
  def GEOMESA_FORCE_COUNT               = propOrDefault("geomesa.force.count")
  def GEOMESA_QUERY_COST_TYPE           = propOrDefault("geomesa.query.cost.type")
  def GEOMESA_QUERY_TIMEOUT_MILLS       = propOrDefault("geomesa.query.timeout.millis")
  def GEOMESA_SCAN_RANGES_TARGET        = propOrDefault("geomesa.scan.ranges.target")
  def GEOMESA_SCAN_RANGES_BATCH         = propOrDefault("geomesa.scan.ranges.batch")
  def GEOMESA_SFT_CONFIG_URLS           = propOrDefault("geomesa.sft.config.urls")
  def GEOMESA_STATS_COMPACT_MILLIS      = propOrDefault("geomesa.stats.compact.millis")

  def propOrDefault(prop: String): String = {
    ensureConfig()
    Option(System.getProperty(prop)).getOrElse{
      Option(props.getProperty(prop)).getOrElse("")
    }
  }

  // For dynamic properties that are not in geomesa.properties
  // This ensures the config is always loaded
  def getProperty(prop: String, default: String = ""): String = {
    ensureConfig()
    Option(System.getProperty(prop)).getOrElse(default)
  }

  def ensureConfig(): Unit = if(! ConfigLoader.isLoaded) ConfigLoader.init()
}
