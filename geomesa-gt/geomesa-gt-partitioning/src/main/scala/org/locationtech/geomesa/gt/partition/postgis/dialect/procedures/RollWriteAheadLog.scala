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

  override def name(info: TypeInfo): FunctionName = FunctionName(s"${info.typeName}_roll_wa")

  override def jobName(info: TypeInfo): SqlLiteral = SqlLiteral(s"${info.typeName}-roll-wa")

  override protected def schedule(info: TypeInfo): SqlLiteral = SqlLiteral("9,19,29,39,49,59 * * * *")

  override protected def invocation(info: TypeInfo): SqlLiteral = SqlLiteral(s"CALL ${name(info).quoted}()")

  override protected def createStatements(info: TypeInfo): Seq[String] =
    Seq(proc(info)) ++ super.createStatements(info)

  private def proc(info: TypeInfo): String = {
    def literalPrefix(string0: String, string1: String, stringN: String*): String =
      literal(string0, string1, stringN ++ Seq(""): _*)

    val table = info.tables.writeAhead
    val writePartition = WriteAheadTable.writesPartition(info).qualified
    val partitionsTable = s"${info.schema.quoted}.${PartitionTablespacesTable.Name.quoted}"

    s"""CREATE OR REPLACE PROCEDURE ${name(info).quoted}() LANGUAGE plpgsql AS
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
       |      LOCK TABLE ONLY ${table.name.qualified} IN SHARE UPDATE EXCLUSIVE MODE;
       |      LOCK TABLE $writePartition IN ACCESS EXCLUSIVE MODE;
       |
       |      -- don't re-create the table if there hasn't been any data inserted
       |      -- call to format fixes errors with non-lower-case identifiers
       |      IF EXISTS(SELECT 1 FROM $writePartition) THEN
       |        SELECT nextval(format('%I', ${literal(table.name.raw, "seq")})) INTO seq_val;
       |
       |        -- format the table name to be 3 digits, with leading zeros as needed
       |        cur_partition := lpad((seq_val - 1)::text, 3, '0');
       |        next_partition := lpad(seq_val::text, 3, '0');
       |        SELECT table_space INTO partition_tablespace FROM $partitionsTable
       |          WHERE type_name = ${literal(info.typeName)} AND table_type = ${WriteAheadTableSuffix.quoted};
       |        IF partition_tablespace IS NULL THEN
       |          partition_tablespace := '';
       |          index_space := '';
       |        ELSE
       |          partition_tablespace := ' TABLESPACE ' || quote_ident(partition_tablespace);
       |          index_space := ' USING INDEX' || partition_tablespace;
       |        END IF;
       |
       |        -- requires ACCESS EXCLUSIVE
       |        EXECUTE 'ALTER TABLE $writePartition RENAME TO ' || quote_ident(${literal(table.name.raw + "_")} || cur_partition);
       |        -- requires SHARE UPDATE EXCLUSIVE
       |        EXECUTE 'CREATE TABLE IF NOT EXISTS $writePartition (' ||
       |          ' CONSTRAINT ' || quote_ident(${literalPrefix(table.name.raw, "pkey")} || next_partition) ||
       |          ' PRIMARY KEY (fid, ${info.cols.dtg.quoted})' || index_space || ')' ||
       |          ' INHERITS (${table.name.qualified})${table.storage.opts}' || partition_tablespace;
       |        EXECUTE 'CREATE INDEX IF NOT EXISTS ' ||
       |          quote_ident(${literalPrefix(table.name.raw, info.cols.dtg.raw)} || next_partition) ||
       |          ' ON $writePartition (${info.cols.dtg.quoted})' || partition_tablespace;
       |${info.cols.geoms.map { col =>
    s"""        EXECUTE 'CREATE INDEX IF NOT EXISTS ' ||
       |          quote_ident(${literalPrefix("spatial", table.name.raw, col.raw.toLowerCase(Locale.US))} || next_partition) ||
       |          ' ON $writePartition USING gist(${col.quoted})' || partition_tablespace;""".stripMargin }.mkString("\n") }
       |${info.cols.indexed.map { col =>
    s"""        EXECUTE 'CREATE INDEX IF NOT EXISTS ' ||
       |          quote_ident(${literalPrefix(table.name.raw, col.raw)} || next_partition) ||
       |          ' ON $writePartition (${col.quoted})' || partition_tablespace;""".stripMargin }.mkString("\n") }
       |
       |        COMMIT; -- releases our locks
       |
       |        EXECUTE 'ANALYZE ' || quote_ident(${literal(table.name.raw + "_")} || cur_partition);
       |
       |      END IF;
       |    END;
       |  $$BODY$$;""".stripMargin
  }
}
