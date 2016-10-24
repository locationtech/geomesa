/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.index.geotools

import java.io.Serializable

import org.geotools.data.DataAccessFactory.Param
import org.locationtech.geomesa.index.conf.QueryProperties
import org.locationtech.geomesa.security.AuditProvider
import org.locationtech.geomesa.utils.audit.AuditWriter

object GeoMesaDataStoreFactory {

  val LooseBBoxParam     = new Param("looseBoundingBox", classOf[java.lang.Boolean], "Use loose bounding boxes - queries will be faster but may return extraneous results", false, true)
  val GenerateStatsParam = new Param("generateStats", classOf[java.lang.Boolean], "Generate data statistics for improved query planning", false, true)
  val AuditQueriesParam  = new Param("auditQueries", classOf[java.lang.Boolean], "Audit queries being run", false, true)
  val CachingParam       = new Param("caching", classOf[java.lang.Boolean], "Cache the results of queries for faster repeated searches. Warning: large result sets can swamp memory", false, false)
  val QueryTimeoutParam  = new Param("queryTimeout", classOf[Integer], "The max time a query will be allowed to run before being killed, in seconds", false)

  implicit class RichParam(val p: Param) extends AnyVal {
    def lookup[T](params: java.util.Map[String, Serializable]): T = p.lookUp(params).asInstanceOf[T]
    def lookupOpt[T](params: java.util.Map[String, Serializable]): Option[T] = Option(p.lookup[T](params))
    def lookupWithDefault[T](params: java.util.Map[String, Serializable]): T =
      p.lookupOpt[T](params).getOrElse(p.getDefaultValue.asInstanceOf[T])
  }

  def queryTimeout(params: java.util.Map[String, Serializable]) = {
    QueryTimeoutParam.lookupOpt[Int](params).map(i => i * 1000L).orElse {
      QueryProperties.QUERY_TIMEOUT_MILLIS.option.map(_.toLong)
    }
  }

  trait GeoMesaDataStoreConfig {
    def catalog: String
    def audit: Option[(AuditWriter, AuditProvider, String)]
    def generateStats: Boolean
    def queryTimeout: Option[Long]
    def looseBBox: Boolean
    def caching: Boolean
  }
}
