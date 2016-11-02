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

  val GEOMESA_CONFIG_FILE_PROP = ConfigLoader.GEOMESA_CONFIG_FILE_PROP
  val GEOMESA_CONFIG_FILE_NAME = ConfigLoader.GEOMESA_CONFIG_FILE_NAME

  def GEOMESA_TOOLS_ACCUMULO_SITE_XML: String = {
    val siteXML = getProperty("geomesa.tools.accumulo.site.xml")
    if (siteXML != null) siteXML
    else s"${System.getenv("ACCUMULO_HOME")}/conf/accumulo-site.xml"
  }
  def GEOMESA_AUDIT_PROVIDER_IMPL       = GeoMesaSystemProperty("geomesa.audit.provider.impl")
  def GEOMESA_AUTH_PROVIDER_IMPL        = GeoMesaSystemProperty("geomesa.auth.provider.impl")
  def GEOMESA_BATCHWRITER_LATENCY_MILLS = GeoMesaSystemProperty("geomesa.batchwriter.latency.millis")
  def GEOMESA_BATCHWRITER_MAXTHREADS    = GeoMesaSystemProperty("geomesa.batchwriter.maxthreads")
  def GEOMESA_BATCHWRITER_MEMORY        = GeoMesaSystemProperty("geomesa.batchwriter.memory")
  def GEOMESA_BATCHWRITER_TIMEOUT_MILLS = GeoMesaSystemProperty("geomesa.batchwriter.timeout.millis")
  def GEOMESA_CONVERT_CONFIG_URLS       = GeoMesaSystemProperty("geomesa.convert.config.urls")
  def GEOMESA_CONVERT_SCRIPTS_PATH      = GeoMesaSystemProperty("geomesa.convert.scripts.path")
  def GEOMESA_FEATURE_ID_GENERATOR      = GeoMesaSystemProperty("geomesa.feature.id-generator")
  def GEOMESA_FORCE_COUNT               = GeoMesaSystemProperty("geomesa.force.count")
  def GEOMESA_QUERY_COST_TYPE           = GeoMesaSystemProperty("geomesa.query.cost.type")
  def GEOMESA_QUERY_TIMEOUT_MILLS       = GeoMesaSystemProperty("geomesa.query.timeout.millis")
  def GEOMESA_SCAN_RANGES_TARGET        = GeoMesaSystemProperty("geomesa.scan.ranges.target")
  def GEOMESA_SCAN_RANGES_BATCH         = GeoMesaSystemProperty("geomesa.scan.ranges.batch")
  def GEOMESA_SFT_CONFIG_URLS           = GeoMesaSystemProperty("geomesa.sft.config.urls")
  def GEOMESA_STATS_COMPACT_MILLIS      = GeoMesaSystemProperty("geomesa.stats.compact.millis")

  case class GeoMesaSystemProperty(property: String, dft: String = null) {
    ensureConfig()
    var default = if (dft != null) dft
                  else Option(getProperty(property)).getOrElse(dft)
    val threadLocalValue = new ThreadLocal[String]()
    def get: String = Option(threadLocalValue.get).getOrElse(Option(System.getProperty(property)).getOrElse(default))
    def option: Option[String] = Option{this.get}
    def set(value: String): Unit = System.setProperty(property, value)
    def clear(): Unit = System.clearProperty(property)
  }

  // For dynamic properties that are not in geomesa-site.xml.template, this is intended
  // to be a System.getProperty drop-in replacement that ensures the config is always loaded.
  def getProperty(prop: String, default: String = null): String = {
    ensureConfig()
    Option(System.getProperty(prop)).getOrElse(default)
  }

  def ensureConfig(): Unit = ConfigLoader.init
}
