/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.jobs.accumulo

import java.io.File

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.Connector
import org.apache.hadoop.conf.Configuration
import org.geotools.data.Query
import org.locationtech.geomesa.accumulo.data.AccumuloQueryPlan.EmptyPlan
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloQueryPlan}
import org.locationtech.geomesa.accumulo.index._
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.index.api.FilterStrategy
import org.locationtech.geomesa.jobs.JobUtils
import org.locationtech.geomesa.utils.classpath.ClassPathUtils
import org.locationtech.geomesa.utils.index.IndexMode
import org.opengis.filter.Filter

import scala.io.Source
import scala.util.Try

object AccumuloJobUtils extends LazyLogging {

  // default jars that will be included with m/r jobs
  lazy val defaultLibJars: List[String] = {
    val defaultLibJarsFile = "org/locationtech/geomesa/jobs/accumulo-libjars.list"
    val url = Try(getClass.getClassLoader.getResource(defaultLibJarsFile))
    val source = url.map(Source.fromURL)
    val lines = source.map(_.getLines().toList)
    source.foreach(_.close())
    lines.get
  }

  // paths are in order of preference for finding a jar
  def defaultSearchPath: Iterator[() => Seq[File]] =
    Iterator(() => ClassPathUtils.getJarsFromEnvironment("GEOMESA_ACCUMULO_HOME", "lib"),
             () => ClassPathUtils.getJarsFromEnvironment("ACCUMULO_HOME"),
             () => ClassPathUtils.getJarsFromClasspath(classOf[AccumuloDataStore]),
             () => ClassPathUtils.getJarsFromClasspath(classOf[Connector]))

  /**
   * Sets the libjars into a Hadoop configuration. Will search the environment first, then the
   * classpath, until all required jars have been found.
   *
   * @param conf job configuration
   * @param libJars jar prefixes to load
   */
  def setLibJars(conf: Configuration,
                 libJars: Seq[String] = defaultLibJars,
                 searchPath: Iterator[() => Seq[File]] = defaultSearchPath): Unit =
    JobUtils.setLibJars(conf, libJars, searchPath)

  /**
   * Gets a query plan that can be satisfied via AccumuloInputFormat - e.g. only 1 table and configuration.
   */
  def getSingleQueryPlan(ds: AccumuloDataStore, query: Query): AccumuloQueryPlan = {
    // disable join plans
    JoinIndex.AllowJoinPlans.set(false)

    try {
      lazy val fallbackIndex = {
        val schema = ds.getSchema(query.getTypeName)
        ds.manager.indices(schema, IndexMode.Read).headOption.getOrElse {
          throw new IllegalStateException(s"Schema '${schema.getTypeName}' does not have any readable indices")
        }
      }

      val queryPlans = ds.getQueryPlan(query)

      if (queryPlans.isEmpty) {
        EmptyPlan(FilterStrategy(fallbackIndex, None, Some(Filter.EXCLUDE), temporal = false, 0L))
      } else if (queryPlans.lengthCompare(1) > 0) {
        // this query requires multiple scans, which we can't execute from some input formats
        // instead, fall back to a full table scan
        logger.warn("Desired query plan requires multiple scans - falling back to full table scan")
        val qps = ds.getQueryPlan(query, Some(fallbackIndex.identifier))
        if (qps.lengthCompare(1) > 0 || qps.exists(_.tables.lengthCompare(1) > 0)) {
          logger.error("The query being executed requires multiple scans, which is not currently " +
              "supported by GeoMesa. Your result set will be partially incomplete. " +
              s"Query: ${filterToString(query.getFilter)}")
        }
        qps.head
      } else {
        val qp = queryPlans.head
        if (qp.tables.lengthCompare(1) > 0) {
          logger.error("The query being executed requires multiple scans, which is not currently " +
              "supported by GeoMesa. Your result set will be partially incomplete. " +
              s"Query: ${filterToString(query.getFilter)}")
        }
        qp
      }
    } finally {
      // make sure we reset the thread locals
      JoinIndex.AllowJoinPlans.remove()
    }
  }

  /**
    * Get a sequence of one or more query plans, which is guaranteed not to contain
    * a JoinPlan (return a fallback in this case). If we get multiple scan plans,
    * that's groovy.
    */
  def getMultipleQueryPlan(ds: AccumuloDataStore, query: Query): Seq[AccumuloQueryPlan] = {
    // disable join plans
    JoinIndex.AllowJoinPlans.set(false)

    try {
      lazy val fallbackIndex = {
        val schema = ds.getSchema(query.getTypeName)
        ds.manager.indices(schema, IndexMode.Read).headOption.getOrElse {
          throw new IllegalStateException(s"Schema '${schema.getTypeName}' does not have any readable indices")
        }
      }

      val queryPlans = ds.getQueryPlan(query)
      if (queryPlans.isEmpty) {
        Seq(EmptyPlan(FilterStrategy(fallbackIndex, None, Some(Filter.EXCLUDE), temporal = false, 0L)))
      } else {
        queryPlans
      }
    } finally {
      // make sure we reset the thread locals
      JoinIndex.AllowJoinPlans.remove()
    }
  }
}
