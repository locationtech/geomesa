/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect
package procedures

import org.locationtech.geomesa.gt.partition.postgis.dialect.tables.{PartitionTablespacesTable, WriteAheadTable}

import java.util.Locale

/**
 * Moves the current write ahead table to a sequential name, and creates a new write ahead table
 * to accept writes going forward
 */
object RollWriteAheadLog extends SqlProcedure with CronSchedule {

  override def name(info: TypeInfo): String = s"${info.name}_roll_wa"

  override def jobName(info: TypeInfo): String = s"${info.name}-roll-wa"

  override protected def schedule(info: TypeInfo): String = "9,19,29,39,49,59 * * * *"

  override protected def invocation(info: TypeInfo): String = s"""CALL "${name(info)}"()"""

  override protected def createStatements(info: TypeInfo): Seq[String] =
    Seq(proc(info)) ++ super.createStatements(info)

  private def proc(info: TypeInfo): String = {
    val table = info.tables.writeAhead
    val writePartition = WriteAheadTable.writesPartition(info)
    s"""CREATE OR REPLACE PROCEDURE "${name(info)}"() LANGUAGE plpgsql AS
       |  $$BODY$$
       |    DECLARE
       |      seq_val smallint;
       |      cur_partition text;        -- rename of the _writes table
       |      next_partition text;       -- next partition that will be created from the _writes table
       |      partition_tablespace text; -- table space command
       |      index_space text;          -- index table space command
       |    BEGIN
       |
       |      -- get the required locks up front to avoid deadlocks with inserts
       |      LOCK TABLE ONLY ${table.name.full} IN SHARE UPDATE EXCLUSIVE MODE;
       |      LOCK TABLE $writePartition IN ACCESS EXCLUSIVE MODE;
       |
       |      -- don't re-create the table if there hasn't been any data inserted
       |      IF EXISTS(SELECT 1 FROM $writePartition) THEN
       |        SELECT nextval('${table.name.raw + "_seq"}') INTO seq_val;
       |
       |        -- format the table name to be 3 digits, with leading zeros as needed
       |        cur_partition := lpad((seq_val - 1)::text, 3, '0');
       |        next_partition := lpad(seq_val::text, 3, '0');
       |        SELECT table_space INTO partition_tablespace FROM "${info.schema}".${PartitionTablespacesTable.TableName}
       |          WHERE type_name = '${info.name}' AND table_type = '$WriteAheadTableSuffix';
       |        IF partition_tablespace IS NULL THEN
       |          partition_tablespace := '';
       |          index_space := '';
       |        ELSE
       |          partition_tablespace := ' TABLESPACE ' || quote_ident(partition_tablespace);
       |          index_space := ' USING INDEX' || partition_tablespace;
       |        END IF;
       |
       |        -- requires ACCESS EXCLUSIVE
       |        EXECUTE 'ALTER TABLE $writePartition RENAME TO ' || quote_ident('${table.name.raw}_' || cur_partition);
       |        -- requires SHARE UPDATE EXCLUSIVE
       |        EXECUTE 'CREATE TABLE IF NOT EXISTS $writePartition (' ||
       |          ' CONSTRAINT ' || quote_ident('${table.name.raw}_pkey_' || next_partition) ||
       |          ' PRIMARY KEY (fid, ${info.cols.dtg.name})' || index_space || ')' ||
       |          ' INHERITS (${table.name.full})${table.storage}' || partition_tablespace;
       |        EXECUTE 'CREATE INDEX IF NOT EXISTS ' ||
       |          quote_ident('${table.name.raw}_${info.cols.dtg.raw}_' || next_partition) ||
       |          ' ON $writePartition (${info.cols.dtg.name})' || partition_tablespace;
       |${info.cols.geoms.map { col =>
    s"""        EXECUTE 'CREATE INDEX IF NOT EXISTS ' ||
       |          quote_ident('spatial_${table.name.raw}_${col.raw.toLowerCase(Locale.US)}_' || next_partition) ||
       |          ' ON $writePartition USING gist(${col.name})' || partition_tablespace;""".stripMargin }.mkString("\n") }
       |${info.cols.indexed.map { col =>
    s"""        EXECUTE 'CREATE INDEX IF NOT EXISTS ' ||
       |          quote_ident('${table.name.raw}_${col.raw}_' || next_partition) ||
       |          ' ON $writePartition (${col.name})' || partition_tablespace;""".stripMargin }.mkString("\n") }
       |
       |        COMMIT; -- releases our locks
       |
       |        EXECUTE 'ANALYZE ' || quote_ident('${table.name.raw}_' || cur_partition);
       |
       |      END IF;
       |    END;
       |  $$BODY$$;""".stripMargin
  }
}
