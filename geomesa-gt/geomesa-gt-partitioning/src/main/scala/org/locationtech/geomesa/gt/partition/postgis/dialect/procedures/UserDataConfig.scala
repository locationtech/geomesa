/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect
package procedures

import org.locationtech.geomesa.gt.partition.postgis.dialect.PartitionedPostgisDialect.SftUserData
import org.locationtech.geomesa.gt.partition.postgis.dialect.tables.UserDataTable

/**
 * Sql snippets for reading feature type user data in a procedure.
 *
 * The user data table is shared between all feature types, and reading from it takes a lock that is held until
 * the end of the transaction. Any ddl on the table will block behind that lock, and will then block all other
 * readers of the table (i.e. all partition maintenance and schema creation). To prevent that, procedures should read
 * all their configuration up front, and then release the lock before doing any long-running work.
 *
 * Snippets are indented to be used in the top-level block of a procedure.
 */
private[procedures] object UserDataConfig {

  private val Indent = "\n      "

  /**
   * Reads user data values into variables, with a single query. Variables will be null if the user data is not set.
   *
   * @param info type info
   * @param values pairs of the variable name to read into, and the user data to read
   * @return
   */
  def select(info: TypeInfo, values: (String, SftUserData[_])*): String = {
    val keys = values.map { case (_, config) => literal(config.key) }
    val cols = keys.map(key => s"max(value) FILTER (WHERE key = $key)")
    Seq(
      s"SELECT ${cols.mkString(", ")}",
      s"  FROM ${info.schema.quoted}.${UserDataTable.Name.quoted}",
      s"  WHERE type_name = ${literal(info.typeName)} AND key IN (${keys.mkString(", ")})",
      s"  INTO ${values.map(_._1).mkString(", ")};"
    ).mkString(Indent)
  }

  /**
   * Converts a tablespace name into the sql clauses for creating tables and indices in that tablespace
   *
   * @param tablespace name of the variable holding the tablespace name, will be updated with the table clause
   * @param index name of the variable to hold the index clause
   * @return
   */
  def tablespaceClauses(tablespace: String, index: String): String = {
    Seq(
      s"IF $tablespace IS NULL THEN",
      s"  $index := '';",
      s"  $tablespace := '';",
      s"ELSE",
      s"  $index := ' USING INDEX TABLESPACE ' || quote_ident($tablespace);",
      s"  $tablespace := ' TABLESPACE ' || quote_ident($tablespace);",
      s"END IF;"
    ).mkString(Indent)
  }

  /**
   * Converts the wal logging flag into the sql clause for creating tables
   *
   * @param variable name of the variable holding the value of `SftUserData.WalLogEnabled`, will be updated
   *                 with the table clause
   * @return
   */
  def walLoggingClause(variable: String): String = {
    Seq(
      s"IF $variable IS NULL OR $variable::boolean THEN",
      s"  $variable := '';",
      s"ELSE",
      s"  $variable := 'UNLOGGED ';",
      s"END IF;"
    ).mkString(Indent)
  }

  /**
   * Releases the lock on the user data table
   *
   * @return
   */
  def release: String = {
    Seq(
      "-- release our lock on the user data table, so that we don't hold it while doing any long-running work -",
      "-- otherwise any ddl on the (shared) user data table will block, and then block all other readers of that table",
      "COMMIT;"
    ).mkString(Indent)
  }
}
