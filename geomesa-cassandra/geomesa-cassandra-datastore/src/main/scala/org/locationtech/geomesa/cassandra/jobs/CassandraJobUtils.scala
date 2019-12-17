/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.jobs

import org.geotools.data.Query
import org.locationtech.geomesa.cassandra.data.{CassandraDataStore, StatementPlan}

object CassandraJobUtils {

  /**
    * Gets a single scan plan, against a single table
    *
    * @param ds data store
    * @param query query
    * @throws java.lang.IllegalArgumentException if query can't be answered with a single scan plan
    * @return
    */
  @throws(classOf[IllegalArgumentException])
  def getSingleScanPlan(ds: CassandraDataStore, query: Query): StatementPlan = {
    // get the query plan to set up the iterators, ranges, etc
    val plans = ds.getQueryPlan(query)
    if (plans.lengthCompare(1) != 0) {
      throw new IllegalArgumentException(s"Query requires multiple query plans: ${plans.mkString(", ")}")
    }

    plans.head match {
      case p: StatementPlan if p.tables.lengthCompare(1) == 0 => p

      case p: StatementPlan =>
        throw new IllegalArgumentException("Query requires multiple tables, which is not supported through " +
          s"the input format: ${p.tables.mkString(", ")}")

      case p =>
        throw new IllegalArgumentException("Query requires a scan which is not supported through " +
          s"the input format: ${p.getClass.getName}")
    }
  }

  /**
    * Gets (potentially) multiple scan plans. Each plan will only scan a single table
    *
    * @param ds data store
    * @param query query
    * @throws java.lang.IllegalArgumentException if query can't be answered with scan plans
    * @return
    */
  @throws(classOf[IllegalArgumentException])
  def getMultiScanPlans(ds: CassandraDataStore, query: Query): Seq[StatementPlan] = {
    ds.getQueryPlan(query).flatMap {
      case p: StatementPlan if p.tables.lengthCompare(1) == 0 => Seq(p)
      case p: StatementPlan => p.tables.map(table => p.copy(tables = Seq(table)))
      case _: StatementPlan => Seq.empty
      case p =>
        throw new IllegalArgumentException("Query requires a scan which is not supported through " +
          s"the input format: ${p.getClass.getName}")
    }
  }
}