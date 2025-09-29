/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.jobs

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.AccumuloClient
import org.apache.hadoop.conf.Configuration
import org.geotools.api.data.Query
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.accumulo.data.AccumuloQueryPlan.EmptyPlan
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloQueryPlan}
import org.locationtech.geomesa.accumulo.index._
import org.locationtech.geomesa.index.api.FilterStrategy
import org.locationtech.geomesa.jobs.JobUtils
import org.locationtech.geomesa.utils.classpath.ClassPathUtils
import org.locationtech.geomesa.utils.index.IndexMode

import java.io.File
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
             () => ClassPathUtils.getJarsFromClasspath(classOf[AccumuloClient]))

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
  @deprecated("Replaced by AccumuloDataStore.getSingleQueryPlan")
  def getSingleQueryPlan(ds: AccumuloDataStore, query: Query): AccumuloQueryPlan = ds.getSingleQueryPlan(query)

  /**
    * Get a sequence of one or more query plans, which is guaranteed not to contain
    * a JoinPlan (return a fallback in this case). If we get multiple scan plans,
    * that's groovy.
    */
  @deprecated("Deprecated with no replacement")
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
        Seq(EmptyPlan(FilterStrategy(fallbackIndex, None, Some(Filter.EXCLUDE), temporal = false, Float.PositiveInfinity)))
      } else {
        queryPlans
      }
    } finally {
      // make sure we reset the thread locals
      JoinIndex.AllowJoinPlans.remove()
    }
  }
}
