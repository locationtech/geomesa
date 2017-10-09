/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.geotools

import org.locationtech.geomesa.index.conf.{QueryProperties, StatsProperties}
import org.locationtech.geomesa.index.geotools.MetadataBackedDataStore.NamespaceConfig
import org.locationtech.geomesa.utils.audit.{AuditProvider, AuditWriter}
import org.locationtech.geomesa.utils.geotools.GeoMesaParam

object GeoMesaDataStoreFactory {

  val QueryThreadsParam  = new GeoMesaParam[Integer]("geomesa.query.threads", "The number of threads to use per query", false, 8, deprecated = Seq("queryThreads", "accumulo.queryThreads"))
  val LooseBBoxParam     = new GeoMesaParam[java.lang.Boolean]("geomesa.query.loose-bounding-box", "Use loose bounding boxes - queries will be faster but may return extraneous results", false, true, deprecated = Seq("looseBoundingBox"))
  val GenerateStatsParam = new GeoMesaParam[java.lang.Boolean]("geomesa.stats.generate", "Generate data statistics for improved query planning", false, true, deprecated = Seq("generateStats"), systemProperty = Some(StatsProperties.GENERATE_STATS, (p) => p.toBoolean))
  val AuditQueriesParam  = new GeoMesaParam[java.lang.Boolean]("geomesa.query.audit", "Audit queries being run", false, true, deprecated = Seq("auditQueries", "collectQueryStats"))
  val CachingParam       = new GeoMesaParam[java.lang.Boolean]("geomesa.query.caching", "Cache the results of queries for faster repeated searches. Warning: large result sets can swamp memory", false, false, deprecated = Seq("caching"))
  val QueryTimeoutParam  = new GeoMesaParam[Integer]("geomesa.query.timeout", "The max time a query will be allowed to run before being killed, in seconds", false, deprecated = Seq("queryTimeout", "accumulo.queryTimeout"), systemProperty = Some((QueryProperties.QUERY_TIMEOUT_MILLIS, (p) => (p.toLong / 1000).toInt)))
  val NamespaceParam     = new GeoMesaParam[String]("namespace", "Namespace")

  trait GeoMesaDataStoreConfig extends NamespaceConfig {
    def catalog: String
    def audit: Option[(AuditWriter, AuditProvider, String)]
    def generateStats: Boolean
    def queryThreads: Int
    def queryTimeout: Option[Long]
    def looseBBox: Boolean
    def caching: Boolean
  }
}
