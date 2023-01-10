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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 8d6784b70d (GEOMESA-3208 Postgis - Fix camel-case feature type names)
=======
>>>>>>> 2eb5450278 (GEOMESA-3208 Postgis - Fix camel-case feature type names)
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a6add7b0b1 (GEOMESA-3208 Postgis - Fix camel-case feature type names)
=======
>>>>>>> b7a3bd4175 (GEOMESA-3208 Postgis - Fix camel-case feature type names)
=======
>>>>>>> ec30c2a23d (GEOMESA-3208 Postgis - Fix camel-case feature type names)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> bf6b23321d (GEOMESA-3208 Postgis - Fix camel-case feature type names)
=======
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 48c6002574 (GEOMESA-3208 Postgis - Fix camel-case feature type names)
=======
>>>>>>> 0f44f9b8ab (GEOMESA-3208 Postgis - Fix camel-case feature type names)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 85b4d3f0dc (GEOMESA-3208 Postgis - Fix camel-case feature type names)
=======
>>>>>>> a9343b6734 (GEOMESA-3208 Postgis - Fix camel-case feature type names)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> bf6b23321d (GEOMESA-3208 Postgis - Fix camel-case feature type names)
=======
>>>>>>> 48c6002574 (GEOMESA-3208 Postgis - Fix camel-case feature type names)
>>>>>>> 25cedeff6a (GEOMESA-3208 Postgis - Fix camel-case feature type names)
=======
<<<<<<< HEAD
=======
>>>>>>> 8d6784b70d (GEOMESA-3208 Postgis - Fix camel-case feature type names)
=======
>>>>>>> 85b4d3f0dc (GEOMESA-3208 Postgis - Fix camel-case feature type names)
=======
>>>>>>> 2eb5450278 (GEOMESA-3208 Postgis - Fix camel-case feature type names)
       |      -- call to format fixes errors with non-lower-case identifiers
       |      IF EXISTS(SELECT 1 FROM $writePartition) THEN
       |        SELECT nextval(format('%I', ${literal(table.name.raw, "seq")})) INTO seq_val;
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 495444cdb7 (GEOMESA-3208 Postgis - Fix camel-case feature type names)
=======
>>>>>>> 25cedeff6a (GEOMESA-3208 Postgis - Fix camel-case feature type names)
=======
>>>>>>> 8d6784b70d (GEOMESA-3208 Postgis - Fix camel-case feature type names)
=======
>>>>>>> 2eb5450278 (GEOMESA-3208 Postgis - Fix camel-case feature type names)
>>>>>>> f639b39b85 (GEOMESA-3208 Postgis - Fix camel-case feature type names)
=======
>>>>>>> f639b39b8 (GEOMESA-3208 Postgis - Fix camel-case feature type names)
>>>>>>> 203dda21b9 (GEOMESA-3208 Postgis - Fix camel-case feature type names)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f639b39b8 (GEOMESA-3208 Postgis - Fix camel-case feature type names)
>>>>>>> 48c6002574 (GEOMESA-3208 Postgis - Fix camel-case feature type names)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f639b39b8 (GEOMESA-3208 Postgis - Fix camel-case feature type names)
>>>>>>> 0f44f9b8ab (GEOMESA-3208 Postgis - Fix camel-case feature type names)
=======
>>>>>>> f639b39b8 (GEOMESA-3208 Postgis - Fix camel-case feature type names)
>>>>>>> 85b4d3f0dc (GEOMESA-3208 Postgis - Fix camel-case feature type names)
<<<<<<< HEAD
=======
>>>>>>> f639b39b85 (GEOMESA-3208 Postgis - Fix camel-case feature type names)
>>>>>>> a9343b6734 (GEOMESA-3208 Postgis - Fix camel-case feature type names)
=======
>>>>>>> 495444cdb7 (GEOMESA-3208 Postgis - Fix camel-case feature type names)
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> f639b39b8 (GEOMESA-3208 Postgis - Fix camel-case feature type names)
>>>>>>> 48c6002574 (GEOMESA-3208 Postgis - Fix camel-case feature type names)
>>>>>>> 25cedeff6a (GEOMESA-3208 Postgis - Fix camel-case feature type names)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
=======
=======
>>>>>>> f639b39b8 (GEOMESA-3208 Postgis - Fix camel-case feature type names)
>>>>>>> 0f44f9b8ab (GEOMESA-3208 Postgis - Fix camel-case feature type names)
>>>>>>> 8d6784b70d (GEOMESA-3208 Postgis - Fix camel-case feature type names)
=======
>>>>>>> 2eb5450278 (GEOMESA-3208 Postgis - Fix camel-case feature type names)
=======
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 48c6002574 (GEOMESA-3208 Postgis - Fix camel-case feature type names)
>>>>>>> d880141fd7 (GEOMESA-3208 Postgis - Fix camel-case feature type names)
=======
=======
>>>>>>> a6add7b0b1 (GEOMESA-3208 Postgis - Fix camel-case feature type names)
=======
>>>>>>> 85b4d3f0dc (GEOMESA-3208 Postgis - Fix camel-case feature type names)
=======
>>>>>>> b7a3bd4175 (GEOMESA-3208 Postgis - Fix camel-case feature type names)
=======
>>>>>>> ec30c2a23d (GEOMESA-3208 Postgis - Fix camel-case feature type names)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> bf6b23321d (GEOMESA-3208 Postgis - Fix camel-case feature type names)
       |      -- call to format fixes errors with non-lower-case identifiers
       |      IF EXISTS(SELECT 1 FROM $writePartition) THEN
       |        SELECT nextval(format('%I', ${literal(table.name.raw, "seq")})) INTO seq_val;
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 495444cdb7 (GEOMESA-3208 Postgis - Fix camel-case feature type names)
=======
>>>>>>> 25cedeff6a (GEOMESA-3208 Postgis - Fix camel-case feature type names)
>>>>>>> f639b39b85 (GEOMESA-3208 Postgis - Fix camel-case feature type names)
<<<<<<< HEAD
>>>>>>> 4d7cc37021 (GEOMESA-3208 Postgis - Fix camel-case feature type names)
=======
=======
>>>>>>> f639b39b8 (GEOMESA-3208 Postgis - Fix camel-case feature type names)
>>>>>>> 203dda21b9 (GEOMESA-3208 Postgis - Fix camel-case feature type names)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d063f807c9 (GEOMESA-3208 Postgis - Fix camel-case feature type names)
=======
=======
>>>>>>> 08f61fab09 (GEOMESA-3208 Postgis - Fix camel-case feature type names)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> bf6b23321d (GEOMESA-3208 Postgis - Fix camel-case feature type names)
=======
>>>>>>> f639b39b8 (GEOMESA-3208 Postgis - Fix camel-case feature type names)
>>>>>>> 48c6002574 (GEOMESA-3208 Postgis - Fix camel-case feature type names)
<<<<<<< HEAD
>>>>>>> d880141fd7 (GEOMESA-3208 Postgis - Fix camel-case feature type names)
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> f639b39b8 (GEOMESA-3208 Postgis - Fix camel-case feature type names)
>>>>>>> 0f44f9b8ab (GEOMESA-3208 Postgis - Fix camel-case feature type names)
<<<<<<< HEAD
>>>>>>> a6add7b0b1 (GEOMESA-3208 Postgis - Fix camel-case feature type names)
=======
=======
>>>>>>> f639b39b8 (GEOMESA-3208 Postgis - Fix camel-case feature type names)
>>>>>>> 85b4d3f0dc (GEOMESA-3208 Postgis - Fix camel-case feature type names)
<<<<<<< HEAD
>>>>>>> b7a3bd4175 (GEOMESA-3208 Postgis - Fix camel-case feature type names)
=======
=======
>>>>>>> f639b39b85 (GEOMESA-3208 Postgis - Fix camel-case feature type names)
>>>>>>> a9343b6734 (GEOMESA-3208 Postgis - Fix camel-case feature type names)
<<<<<<< HEAD
>>>>>>> ec30c2a23d (GEOMESA-3208 Postgis - Fix camel-case feature type names)
=======
=======
>>>>>>> 495444cdb7 (GEOMESA-3208 Postgis - Fix camel-case feature type names)
<<<<<<< HEAD
>>>>>>> 08f61fab09 (GEOMESA-3208 Postgis - Fix camel-case feature type names)
=======
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> f639b39b8 (GEOMESA-3208 Postgis - Fix camel-case feature type names)
>>>>>>> 48c6002574 (GEOMESA-3208 Postgis - Fix camel-case feature type names)
>>>>>>> 25cedeff6a (GEOMESA-3208 Postgis - Fix camel-case feature type names)
>>>>>>> bf6b23321d (GEOMESA-3208 Postgis - Fix camel-case feature type names)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
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
