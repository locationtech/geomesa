/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.jobs

import java.io.File

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.Connector
import org.apache.hadoop.conf.Configuration
import org.geotools.data.Query
import org.locationtech.geomesa.accumulo.GeomesaSystemProperties.QueryProperties
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.index._
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.index.api.FilterStrategy
import org.locationtech.geomesa.utils.classpath.ClassPathUtils
import org.locationtech.geomesa.utils.index.IndexMode
import org.opengis.filter.Filter

import scala.io.Source
import scala.util.Try

object JobUtils extends LazyLogging {

  // default jars that will be included with m/r jobs
  lazy val defaultLibJars = {
    val defaultLibJarsFile = "org/locationtech/geomesa/jobs/default-libjars.list"
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
                 searchPath: Iterator[() => Seq[File]] = defaultSearchPath): Unit = {
    val paths = ClassPathUtils.findJars(libJars, searchPath).map(f => "file:///" + f.getAbsolutePath)
    // tmpjars is the hadoop config that corresponds to libjars
    conf.setStrings("tmpjars", paths: _*)
    logger.debug(s"Job will use the following libjars=${paths.mkString("\n", "\n", "")}")
  }

  /**
   * Gets a query plan that can be satisfied via AccumuloInputFormat - e.g. only 1 table and configuration.
   */
  def getSingleQueryPlan(ds: AccumuloDataStore, query: Query): QueryPlan = {
    // disable range batching for this request
    QueryProperties.SCAN_BATCH_RANGES.threadLocalValue.set(Int.MaxValue.toString)

    try {
      lazy val fallbackIndex = {
        val schema = ds.getSchema(query.getTypeName)
        AccumuloFeatureIndex.indices(schema, IndexMode.Read).headOption.getOrElse {
          throw new IllegalStateException(s"Schema '${schema.getTypeName}' does not have any readable indices")
        }
      }

      val queryPlans = ds.getQueryPlan(query)

      if (queryPlans.isEmpty) {
        EmptyPlan(FilterStrategy(fallbackIndex, None, Some(Filter.EXCLUDE)))
      } else if (queryPlans.length > 1 || queryPlans.head.isInstanceOf[JoinPlan]) {
        // this query has a join or requires multiple scans, which we can't execute from input formats
        // instead, fall back to a full table scan
        logger.warn("Desired query plan requires multiple scans - falling back to full table scan")
        val qps = ds.getQueryPlan(query, Some(fallbackIndex))
        if (qps.length > 1) {
          logger.error("The query being executed requires multiple scans, which is not currently " +
              "supported by geomesa. Your result set will be partially incomplete. " +
              s"Query: ${filterToString(query.getFilter)}")
        }
        qps.head
      } else {
        queryPlans.head
      }
    } finally {
      // make sure we reset the thread local
      QueryProperties.SCAN_BATCH_RANGES.threadLocalValue.remove()
    }
  }
}
