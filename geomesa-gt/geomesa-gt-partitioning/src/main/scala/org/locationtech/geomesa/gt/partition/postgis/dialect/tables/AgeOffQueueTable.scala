/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect
package tables

/**
 * Expired partitions awaiting concurrent detachment and deletion
 */
object AgeOffQueueTable extends SqlStatements {

  override protected def createStatements(info: TypeInfo): Seq[String] = Seq(
    s"""CREATE TABLE IF NOT EXISTS ${info.tables.ageOffQueue.name.qualified} (
       |  source_partition text PRIMARY KEY,
       |  partition_oid oid NOT NULL,
       |  parent_oid oid NOT NULL,
       |  enqueued timestamp without time zone NOT NULL
       |);""".stripMargin
  )

  override protected def dropStatements(info: TypeInfo): Seq[String] = Seq(
    s"""DO $$$$
       |DECLARE partition_name text;
       |BEGIN
       |  -- Detached partitions are no longer removed by dropping their parent.
       |  IF to_regclass(${literal(info.tables.ageOffQueue.name.qualified)}) IS NOT NULL THEN
       |    FOR partition_name IN
       |      SELECT source_partition FROM ${info.tables.ageOffQueue.name.qualified} ORDER BY source_partition
       |    LOOP
       |      EXECUTE 'DROP TABLE IF EXISTS ' || partition_name;
       |    END LOOP;
       |  END IF;
       |END$$$$;""".stripMargin,
    s"DROP TABLE IF EXISTS ${info.tables.ageOffQueue.name.qualified};"
  )
}
