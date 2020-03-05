/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.jobs

import org.geotools.data.Query
import org.locationtech.geomesa.hbase.data.HBaseDataStore
import org.locationtech.geomesa.hbase.data.HBaseQueryPlan.{EmptyPlan, ScanPlan}

object HBaseJobUtils {

  /**
    * Gets a single scan plan, against a single table
    *
    * @param ds data store
    * @param query query
    * @throws java.lang.IllegalArgumentException if query can't be answered with a single scan plan
    * @return
    */
  @throws(classOf[IllegalArgumentException])
  def getSingleScanPlan(ds: HBaseDataStore, query: Query): ScanPlan = {
    // get the query plan to set up the iterators, ranges, etc
    val plans = ds.getQueryPlan(query)
    if (plans.lengthCompare(1) != 0) {
      throw new IllegalArgumentException(s"Query requires multiple query plans: ${plans.mkString(", ")}")
    }

    plans.head match {
      case p: ScanPlan if p.scans.lengthCompare(1) == 0 => p

      case p: ScanPlan =>
          throw new IllegalArgumentException("Query requires multiple tables, which is not supported through " +
              s"the input format: ${p.scans.map(_.table).mkString(", ")}")

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
  def getMultiScanPlans(ds: HBaseDataStore, query: Query): Seq[ScanPlan] = {
    ds.getQueryPlan(query).flatMap {
      case p: ScanPlan if p.scans.lengthCompare(1) == 0 => Seq(p)
      case p: ScanPlan => p.scans.map(scan => p.copy(scans = Seq(scan)))
      case _: EmptyPlan => Seq.empty
      case p =>
        throw new IllegalArgumentException("Query requires a scan which is not supported through " +
          s"the input format: ${p.getClass.getName}")
    }
  }
}
