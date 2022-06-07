/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect
package procedures

import org.locationtech.geomesa.gt.partition.postgis.dialect.tables.{PartitionTablespacesTable, SequenceTable}

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
    val table = info.tables.writeAhead
    val partitionsTable = s"${info.schema.quoted}.${PartitionTablespacesTable.Name.quoted}"

    s"""CREATE OR REPLACE PROCEDURE ${name(info).quoted}() LANGUAGE plpgsql AS
       |  $$BODY$$
       |    DECLARE
       |      seq_val smallint;
       |      cur_partition text;        -- rename of the _writes table
       |      next_partition text;       -- next partition that will be created from the _writes table
       |      partition_tablespace text; -- table space command
       |      index_space text;          -- index table space command
       |      pexists boolean;           -- table exists check
       |    BEGIN
       |
       |      SELECT value FROM ${info.schema.quoted}.${SequenceTable.Name.quoted}
       |        WHERE type_name = ${literal(info.typeName)} INTO seq_val;
       |      cur_partition := ${literal(table.name.raw + "_")} || lpad(seq_val::text, 3, '0');
       |
       |      -- get the required locks up front to avoid deadlocks with inserts
       |      LOCK TABLE ONLY ${table.name.qualified} IN SHARE UPDATE EXCLUSIVE MODE;
       |
       |      -- don't re-create the table if there hasn't been any data inserted
<<<<<<< HEAD
       |      EXECUTE 'SELECT EXISTS(SELECT 1 FROM ${info.schema.quoted}.' || quote_ident(cur_partition) || ')'
       |          INTO pexists;
       |      IF pexists THEN
       |
       |        IF seq_val < 999 THEN
       |          seq_val := seq_val + 1;
       |        ELSE
       |          seq_val := 0;
       |        END IF;
       |
       |        UPDATE ${info.schema.quoted}.${SequenceTable.Name.quoted} SET value = seq_val
       |          WHERE type_name = ${literal(info.typeName)};
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 48c6002574 (GEOMESA-3208 Postgis - Fix camel-case feature type names)
=======
       |      -- call to format fixes errors with non-lower-case identifiers
       |      IF EXISTS(SELECT 1 FROM $writePartition) THEN
       |        SELECT nextval(format('%I', ${literal(table.name.raw, "seq")})) INTO seq_val;
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f639b39b85 (GEOMESA-3208 Postgis - Fix camel-case feature type names)
=======
>>>>>>> f639b39b8 (GEOMESA-3208 Postgis - Fix camel-case feature type names)
>>>>>>> 203dda21b9 (GEOMESA-3208 Postgis - Fix camel-case feature type names)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f639b39b8 (GEOMESA-3208 Postgis - Fix camel-case feature type names)
>>>>>>> 48c6002574 (GEOMESA-3208 Postgis - Fix camel-case feature type names)
       |
       |        -- format the table name to be 3 digits, with leading zeros as needed
       |        next_partition := ${literal(table.name.raw + "_")} || lpad(seq_val::text, 3, '0');
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
       |        -- requires SHARE UPDATE EXCLUSIVE
       |        EXECUTE 'CREATE TABLE IF NOT EXISTS ${info.schema.quoted}.' || quote_ident(next_partition) || '(' ||
       |          'CONSTRAINT ' || quote_ident(next_partition || '_pkey') ||
       |          ' PRIMARY KEY (fid, ${info.cols.dtg.quoted})' || index_space || ')' ||
       |          ' INHERITS (${table.name.qualified})${table.storage.opts}' || partition_tablespace;
       |        EXECUTE 'CREATE INDEX IF NOT EXISTS ' || quote_ident(next_partition || '_' || ${info.cols.dtg.asLiteral}) ||
       |          ' ON ${info.schema.quoted}.' || quote_ident(next_partition) || ' (${info.cols.dtg.quoted})' ||
       |          partition_tablespace;
       |${info.cols.geoms.map { col =>
    s"""        EXECUTE 'CREATE INDEX IF NOT EXISTS ' || quote_ident(next_partition || '_spatial_' || ${col.asLiteral}) ||
       |          ' ON ${info.schema.quoted}.' || quote_ident(next_partition) || ' USING gist(${col.quoted})' ||
       |          partition_tablespace;""".stripMargin}.mkString("\n")}
       |${info.cols.indexed.map { col =>
    s"""        EXECUTE 'CREATE INDEX IF NOT EXISTS ' || quote_ident(next_partition || '_' || ${col.asLiteral}) ||
       |          ' ON ${info.schema.quoted}.' || quote_ident(next_partition) || '(${col.quoted})' ||
       |          partition_tablespace;""".stripMargin}.mkString("\n")}
       |
       |        COMMIT; -- releases our locks
       |
       |        EXECUTE 'ANALYZE ' || quote_ident(cur_partition);
       |
       |      END IF;
       |    END;
       |  $$BODY$$;""".stripMargin
  }
}
