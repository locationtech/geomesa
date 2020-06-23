/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.jobs

import org.geotools.data.Query
import org.locationtech.geomesa.cassandra.data.{CassandraDataStore, EmptyPlan, StatementPlan}

object CassandraJobUtils {
  /**
   * Gets (potentially) multiple scan plans. Each plan will only scan a single table
   *
   * @param ds data store
   * @param query query
   * @throws java.lang.IllegalArgumentException if query can't be answered with scan plans
   * @return
   */
  @throws(classOf[IllegalArgumentException])
  def getMultiStatementPlans(ds: CassandraDataStore, query: Query): Seq[StatementPlan] = {
    ds.getQueryPlan(query).flatMap {
      case p: StatementPlan if p.tables.lengthCompare(1) == 0 => Seq(p)
      case p: StatementPlan => p.tables.map(table => p.copy(tables = Seq(table)))
      case _: EmptyPlan => Seq.empty
      case p =>
        throw new IllegalArgumentException("Query requires a scan which is not supported through " +
          s"the input format: ${p.getClass.getName}")
    }
  }
}
