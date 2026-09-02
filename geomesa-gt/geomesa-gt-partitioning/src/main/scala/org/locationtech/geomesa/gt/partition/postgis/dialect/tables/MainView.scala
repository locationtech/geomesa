/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect
package tables

import org.locationtech.geomesa.gt.partition.postgis.dialect.auths.SessionDataSource

/**
 * Main view of all the partitions and write ahead table. This should accept and reads and writes.
 */
object MainView extends SqlStatements {

  override protected def createStatements(info: TypeInfo): Seq[String] = {
    // if visibilities are enabled, filter each branch by evaluating the hidden '_vis' column against the
    // caller's authorizations, stamped into the 'geomesa.auths' session variable by SessionDataSource.
    // the auths array is built in an uncorrelated sub-select so that it's evaluated once per query (as an
    // init plan) rather than re-running current_setting/string_to_array for every row
    val filter = info.cols.vis match {
      case None => ""
      case Some(vis) =>
        s" WHERE pg_vis(${vis.quoted}, (SELECT string_to_array(current_setting('${SessionDataSource.AuthConfigName}', true), ',')))"
    }
    Seq(
      s"""CREATE OR REPLACE VIEW ${info.tables.view.name.qualified} AS
         |  SELECT * FROM ${info.tables.writeAhead.name.qualified}$filter UNION ALL
         |  SELECT * FROM ${info.tables.writeAheadPartitions.name.qualified}$filter UNION ALL
         |  SELECT * FROM ${info.tables.mainPartitions.name.qualified}$filter UNION ALL
         |  SELECT * FROM ${info.tables.spillPartitions.name.qualified}$filter;""".stripMargin
    )
  }

  override protected def dropStatements(info: TypeInfo): Seq[String] =
    Seq(s"DROP VIEW IF EXISTS ${info.tables.view.name.qualified};")
}
