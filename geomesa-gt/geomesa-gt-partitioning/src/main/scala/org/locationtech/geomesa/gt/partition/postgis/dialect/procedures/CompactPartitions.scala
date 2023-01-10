/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 1463162d60 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9f430502b2 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> dce8c58b44 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 0bd247219b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 847c6dae88 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> b727e40f7c (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 3515f7f054 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 0bd247219b (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 847c6dae88 (GEOMESA-3254 Add Bloop build support)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect
package procedures

import org.locationtech.geomesa.gt.partition.postgis.dialect.tables.PartitionTablespacesTable

/**
 * Re-sorts a partition table. Can be used for maintenance if time-latent data comes in and degrades performance
 * of the BRIN index
 */
object CompactPartitions extends SqlProcedure {

  override def name(info: TypeInfo): FunctionName = FunctionName(s"${info.typeName}_compact_partitions")

  override protected def createStatements(info: TypeInfo): Seq[String] = Seq(proc(info))

  private def proc(info: TypeInfo): String = {
    val dtgCol = info.cols.dtg.quoted
    val geomCol = info.cols.geom.quoted
    val hours = info.partitions.hoursPerPartition
    val partitionsTable = s"${info.schema.quoted}.${PartitionTablespacesTable.Name.quoted}"
    s"""CREATE OR REPLACE PROCEDURE ${name(info).quoted}(for_date timestamp without time zone) LANGUAGE plpgsql AS
       |  $$BODY$$
       |    DECLARE
       |      partition_name text;
       |      spill_partition text;
       |      min_dtg timestamp without time zone;         -- min date in our partitioned tables
       |      partition_start timestamp without time zone; -- start bounds for the partition we're writing
       |      partition_end timestamp without time zone;   -- end bounds for the partition we're writing
       |      partition_tablespace text;
       |      index_tablespace text;                       -- index tablespace
       |      unsorted_count bigint;
       |      pexists boolean;                             -- table exists check
       |    BEGIN
       |      IF for_date IS NULL THEN
       |        RAISE EXCEPTION 'date is null' USING HINT = 'Please use a valid date';
       |      END IF;
       |
       |      partition_name :=  ${info.tables.mainPartitions.name.asLiteral} || '_' ||
       |        to_char(truncate_to_partition(for_date, $hours), 'YYYY_MM_DD_HH24');
       |
       |      SELECT table_space INTO partition_tablespace FROM $partitionsTable
       |        WHERE type_name = ${literal(info.typeName)} AND table_type = ${PartitionedTableSuffix.quoted};
       |      IF partition_tablespace IS NULL THEN
       |        index_tablespace := '';
       |        partition_tablespace := '';
       |      ELSE
       |        index_tablespace := ' USING INDEX TABLESPACE '|| quote_ident(partition_tablespace);
       |        partition_tablespace := ' TABLESPACE ' || quote_ident(partition_tablespace);
       |      END IF;
       |
       |      RAISE INFO '% Compacting partition table %', timeofday()::timestamp, partition_name;
       |      LOCK TABLE ONLY ${info.tables.mainPartitions.name.qualified} IN SHARE UPDATE EXCLUSIVE MODE;
       |      -- lock the child table to prevent any inserts that would be lost
       |      EXECUTE 'LOCK TABLE ${info.schema.quoted}.' || quote_ident(partition_name) ||
       |        ' IN SHARE ROW EXCLUSIVE MODE';
       |      EXECUTE 'SELECT $dtgCol FROM ${info.schema.quoted}.' || quote_ident(partition_name) ||
       |        ' LIMIT 1' INTO min_dtg;
       |      partition_start := truncate_to_partition(min_dtg, $hours);
       |      partition_end := partition_start + INTERVAL '$hours HOURS';
       |      spill_partition := ${info.tables.spillPartitions.name.asLiteral} || '_' || to_char(partition_start, 'YYYY_MM_DD_HH24');
       |
       |      SELECT EXISTS(SELECT FROM pg_tables WHERE schemaname = ${info.schema.asLiteral} AND tablename = spill_partition)
       |        INTO pexists;
       |
       |      -- use "create table as" (vs create then insert) for performance benefits related to WAL skipping
       |      IF pexists THEN
       |        -- lock the child table to prevent any inserts that would be lost
       |        EXECUTE 'LOCK TABLE ${info.schema.quoted}.' || quote_ident(spill_partition) ||
       |          ' IN SHARE ROW EXCLUSIVE MODE';
       |        EXECUTE 'CREATE TABLE ${info.schema.quoted}.' || quote_ident(partition_name || '_tmp_sort') ||
       |          partition_tablespace || ' AS SELECT * FROM' ||
       |          ' (SELECT * FROM ${info.schema.quoted}.' || quote_ident(partition_name) ||
       |          ' UNION ALL SELECT * FROM ${info.schema.quoted}.' || quote_ident(spill_partition) ||
       |          ') results' ||
       |          ' ORDER BY _st_sortablehash($geomCol)';
       |        GET DIAGNOSTICS unsorted_count := ROW_COUNT;
       |      ELSE
       |        EXECUTE 'CREATE TABLE ${info.schema.quoted}.' || quote_ident(partition_name || '_tmp_sort') ||
       |          partition_tablespace || ' AS SELECT * FROM ' || quote_ident(partition_name) ||
       |          ' ORDER BY _st_sortablehash($geomCol)';
       |        GET DIAGNOSTICS unsorted_count := ROW_COUNT;
       |      END IF;
       |
       |      EXECUTE 'ALTER TABLE ${info.schema.quoted}.' || quote_ident(partition_name || '_tmp_sort') ||
       |        ' ADD CONSTRAINT ' || quote_ident(partition_name || '_pkey_tmp_sort') ||
       |        ' PRIMARY KEY (fid, $dtgCol)' || index_tablespace;
       |      -- creating a constraint allows it to be attached to the parent without any additional checks
       |      EXECUTE 'ALTER TABLE  ${info.schema.quoted}.' || quote_ident(partition_name || '_tmp_sort') ||
       |        ' ADD CONSTRAINT ' || quote_ident(partition_name || '_constraint_tmp_sort') ||
       |        ' CHECK ( $dtgCol >= ' || quote_literal(partition_start) ||
       |        ' AND $dtgCol < ' || quote_literal(partition_end) || ' )';
       |
       |      -- create indices before attaching to minimize time to attach, copied from PartitionTables code
       |      EXECUTE 'CREATE INDEX IF NOT EXISTS ' || quote_ident(partition_name || '_${info.cols.geom.raw}_tmp_sort') ||
       |        ' ON ${info.schema.quoted}.' || quote_ident(partition_name || '_tmp_sort') ||
       |        ' USING BRIN($geomCol)' || partition_tablespace;
       |      EXECUTE 'CREATE INDEX IF NOT EXISTS ' || quote_ident(partition_name || '_${info.cols.dtg.raw}_tmp_sort') ||
       |        ' ON ${info.schema.quoted}.' || quote_ident(partition_name || '_tmp_sort') ||
       |        ' ($dtgCol)' || partition_tablespace;
       |${info.cols.indexed.map { col =>
    s"""      EXECUTE 'CREATE INDEX IF NOT EXISTS ' || quote_ident(partition_name || '_${col.raw}_tmp_sort') ||
       |        ' ON ${info.schema.quoted}.' || quote_ident(partition_name || '_tmp_sort') ||
       |        ' (${col.quoted})' || partition_tablespace;""".stripMargin}.mkString("\n")}
       |
       |      RAISE INFO '% Dropping old partition table (queries will be blocked) %', timeofday()::timestamp, partition_name;
       |      EXECUTE 'DROP TABLE IF EXISTS ${info.schema.quoted}.' || quote_ident(partition_name);
       |      IF pexists THEN
       |        EXECUTE 'DROP TABLE IF EXISTS ${info.schema.quoted}.' || quote_ident(spill_partition);
       |      END IF;
       |
       |      RAISE INFO '% Renaming newly sorted partition table %', timeofday()::timestamp, partition_name;
       |      EXECUTE 'ALTER TABLE ${info.schema.quoted}.' || quote_ident(partition_name || '_tmp_sort') ||
       |        ' RENAME TO ' || quote_ident(partition_name);
       |      EXECUTE 'ALTER TABLE ${info.schema.quoted}.' || quote_ident(partition_name) ||
       |        ' RENAME CONSTRAINT ' || quote_ident(partition_name || '_pkey_tmp_sort') ||
       |        ' TO ' || quote_ident(partition_name || '_pkey');
       |      EXECUTE 'ALTER INDEX ' || quote_ident(partition_name || '_${info.cols.geom.raw}_tmp_sort') ||
       |        ' RENAME TO ' || quote_ident(partition_name || '_${info.cols.geom.raw}');
       |      EXECUTE 'ALTER INDEX ' || quote_ident(partition_name || '_${info.cols.dtg.raw}_tmp_sort') ||
       |        ' RENAME TO ' || quote_ident(partition_name || '_${info.cols.dtg.raw}');
       |${info.cols.indexed.map { col =>
    s"""      EXECUTE 'ALTER INDEX ' || quote_ident(partition_name || '_${col.raw}_tmp_sort') ||
       |        ' RENAME TO ' || quote_ident(partition_name || '_${col.raw}');""".stripMargin}.mkString("\n")}
       |
       |      RAISE INFO '% Attaching newly sorted partition table %', timeofday()::timestamp, partition_name;
       |      EXECUTE 'ALTER TABLE ${info.tables.mainPartitions.name.qualified}' ||
       |        ' ATTACH PARTITION ${info.schema.quoted}.' || quote_ident(partition_name) ||
       |        ' FOR VALUES FROM (' || quote_literal(partition_start) || ') TO (' ||
       |        quote_literal(partition_end) || ' );';
       |      RAISE INFO '% Done compacting partition table %', timeofday()::timestamp, partition_name;
       |
       |      -- now that we've attached the table we can drop the redundant constraint
       |      RAISE INFO '% Dropping constraint for partition table %', timeofday()::timestamp, partition_name;
       |      EXECUTE 'ALTER TABLE ${info.schema.quoted}.' || quote_ident(partition_name) ||
       |        ' DROP CONSTRAINT ' || quote_ident(partition_name || '_constraint_tmp_sort');
       |
       |      -- mark the partition to be analyzed in a separate thread
       |      INSERT INTO ${info.tables.analyzeQueue.name.qualified}(partition_name, enqueued)
       |        VALUES (partition_name, now());
       |
       |      -- commit to release our locks
       |      COMMIT;
       |      RAISE INFO '% Done compacting % rows in partition %', timeofday()::timestamp, unsorted_count, partition_name;
       |
       |    END;
       |  $$BODY$$;
       |""".stripMargin
  }
}
