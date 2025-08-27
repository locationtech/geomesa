/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
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
 * Merge recent write-ahead partitions and move them into the main partition table
 */
object MergeWriteAheadPartitions extends SqlProcedure {

  override def name(info: TypeInfo): FunctionName = FunctionName(s"${info.typeName}_merge_wa_partitions")

  override protected def createStatements(info: TypeInfo): Seq[String] = Seq(proc(info))

  private def proc(info: TypeInfo): String = {
    s"""CREATE OR REPLACE PROCEDURE ${info.schema.quoted}.${name(info).quoted}(cur_time timestamp without time zone) LANGUAGE plpgsql AS
       |  $$BODY$$
       |    DECLARE
       |      partition_size int;                          -- size of the partition, in hours
       |      min_dtg timestamp without time zone;         -- min date in our partitioned tables
       |      main_cutoff timestamp without time zone;     -- max age of the records for main tables
       |      partition_start timestamp without time zone; -- start bounds for the partition we're writing
       |      partition_end timestamp without time zone;   -- end bounds for the partition we're writing
       |      partition_name text;                         -- partition table name
       |      partition_parent text;                       -- partition table to attach to
       |      partition_tablespace text;                   -- partition tablespace
       |      index_tablespace text;                       -- index tablespace
       |      table_wa_logging text;                       -- wa log options
       |      write_ahead_partitions text[];               -- names of the partitions we're migrating
       |      write_ahead_partition text;                  -- name of current partition
       |      pexists boolean;                             -- table exists check
       |      unsorted_count bigint;
       |    BEGIN
       |      -- constants
       |      SELECT COALESCE(
       |          (SELECT value::int FROM ${info.schema.quoted}.${UserDataTable.Name.quoted}
       |            WHERE type_name = ${literal(info.typeName)} AND key = ${literal(SftUserData.IntervalHours.key)}),
       |          ${SftUserData.IntervalHours.default}
       |        ) INTO partition_size;
       |      main_cutoff := ${info.schema.quoted}.truncate_to_partition(cur_time, partition_size) - make_interval(hours => partition_size);
       |
       |      -- move data from the write ahead partitions to the main partitions
       |      LOOP
       |        -- find the range of dates in the write ahead partition tables
       |        SELECT min(${info.cols.dtg.quoted}) INTO min_dtg FROM ${info.tables.writeAheadPartitions.name.qualified}
       |          WHERE ${info.cols.dtg.quoted} < main_cutoff;
       |        EXIT WHEN min_dtg IS NULL;
       |
       |        partition_start := ${info.schema.quoted}.truncate_to_partition(min_dtg, partition_size);
       |        partition_end := partition_start + make_interval(hours => partition_size);
       |        partition_parent := ${info.tables.mainPartitions.name.asLiteral};
       |        partition_name := partition_parent || '_' || to_char(partition_start, 'YYYY_MM_DD_HH24');
       |
       |        SELECT EXISTS(SELECT FROM pg_tables WHERE schemaname = ${info.schema.asLiteral} AND tablename = partition_name)
       |          INTO pexists;
       |
       |        -- if the partition already exists, write to the spill partition instead to avoid messing up the BRIN index
       |        IF pexists THEN
       |          partition_parent := ${info.tables.spillPartitions.name.asLiteral};
       |          partition_name := partition_parent || '_' || to_char(partition_start, 'YYYY_MM_DD_HH24');
       |          SELECT EXISTS(SELECT FROM pg_tables WHERE schemaname = ${info.schema.asLiteral} AND tablename = partition_name)
       |            INTO pexists;
       |        END IF;
       |
       |        -- find the write ahead partitions we're copying from
       |        -- order the results to ensure we get locks in a consistent order to avoid deadlocks
       |        write_ahead_partitions := Array(
       |          SELECT '${info.schema.quoted}.' || quote_ident(pg_class.relname)
       |            FROM pg_catalog.pg_inherits
       |            INNER JOIN pg_catalog.pg_class ON (pg_inherits.inhrelid = pg_class.oid)
       |            INNER JOIN pg_catalog.pg_namespace ON (pg_class.relnamespace = pg_namespace.oid)
       |            WHERE inhparent = ${info.tables.writeAheadPartitions.name.asRegclass}
       |              AND pg_class.relname >= ${info.tables.writeAheadPartitions.name.asLiteral} || '_' || to_char(partition_start, 'YYYY_MM_DD_HH24_MI')
       |              AND pg_class.relname < ${info.tables.writeAheadPartitions.name.asLiteral} || '_' || to_char(partition_end, 'YYYY_MM_DD_HH24_MI')
       |            ORDER BY 1
       |          );
       |
       |        -- get a lock on the tables - this mode won't prevent reads but will prevent writes
       |        -- (there shouldn't be any writes though) and will synchronize this method
       |        -- TODO we really just need to sync this method for safety in manual invocations
       |        -- FOREACH write_ahead_partition IN ARRAY write_ahead_partitions LOOP
       |        --   EXECUTE 'LOCK TABLE ' || write_ahead_partition || ' IN SHARE ROW EXCLUSIVE MODE';
       |        --   RAISE INFO '% Locked write ahead partition % for migration', timeofday()::timestamp, write_ahead_partition;
       |        -- END LOOP;
       |
       |        -- create a view from the tables so that we can sort the result by an expression (geohash)
       |        EXECUTE 'CREATE TEMP VIEW ' || quote_ident(partition_name || '_tmp_migrate') ||
       |          ' AS SELECT * FROM ' || array_to_string(write_ahead_partitions, ' UNION ALL SELECT * FROM ');
       |
       |        -- copy rows from write ahead partitions to main partition table
       |        IF pexists THEN
       |          RAISE INFO '% Copying rows to partition %', timeofday()::timestamp, partition_name;
       |          EXECUTE 'INSERT INTO ${info.schema.quoted}.' || quote_ident(partition_name) ||
       |            ' SELECT * FROM ' || quote_ident(partition_name || '_tmp_migrate') ||
       |            '   ORDER BY _st_sortablehash(${info.cols.geom.quoted})' ||
       |            '   ON CONFLICT DO NOTHING';
       |          GET DIAGNOSTICS unsorted_count := ROW_COUNT;
       |        ELSE
       |          RAISE INFO '% Creating partition with insert % (unattached)', timeofday()::timestamp, partition_name;
       |
       |          SELECT value FROM ${info.schema.quoted}.${UserDataTable.Name.quoted}
       |            WHERE type_name = ${literal(info.typeName)} AND key = ${literal(SftUserData.MainTableSpace.key)}
       |            INTO partition_tablespace;
       |          IF partition_tablespace IS NULL THEN
       |            index_tablespace := '';
       |            partition_tablespace := '';
       |          ELSE
       |            index_tablespace := ' USING INDEX TABLESPACE '|| quote_ident(partition_tablespace);
       |            partition_tablespace := ' TABLESPACE ' || quote_ident(partition_tablespace);
       |          END IF;
       |
       |          SELECT COALESCE(
       |            (SELECT value FROM ${info.schema.quoted}.${UserDataTable.Name.quoted}
       |              WHERE type_name = ${literal(info.typeName)} AND key = ${literal(SftUserData.WalLogEnabled.key)}),
       |              '${SftUserData.WalLogEnabled.default}'
       |            ) INTO table_wa_logging;
       |          IF table_wa_logging IS NULL OR NOT table_wa_logging::boolean THEN
       |            table_wa_logging := '';
       |          ELSE
       |            table_wa_logging := 'UNLOGGED ';
       |          END IF;
       |
       |          -- upper bounds are exclusive
       |          -- this won't have any indices until we attach it to the parent partition table
       |          -- use "create table as" (vs create then insert) for performance benefits related to WAL skipping
       |          -- we need a "select distinct" to avoid primary key conflicts - this should be fairly cheap since
       |          --   we're already sorting and there should be few or no conflicts
       |          -- create the partition table with a 'create as' for improved performance
       |          EXECUTE 'CREATE ' || table_wa_logging || 'TABLE ${info.schema.quoted}.' || quote_ident(partition_name) ||
       |            partition_tablespace || ' AS SELECT DISTINCT ON' ||
       |            ' (_st_sortablehash(${info.cols.geom.quoted}), fid, ${info.cols.dtg.quoted}) * FROM ' ||
       |            quote_ident(partition_name || '_tmp_migrate') || ' ORDER BY _st_sortablehash(${info.cols.geom.quoted})';
       |          GET DIAGNOSTICS unsorted_count := ROW_COUNT;
       |          EXECUTE 'ALTER TABLE ${info.schema.quoted}.' || quote_ident(partition_name) ||
       |            ' ADD CONSTRAINT ' || quote_ident(partition_name || '_pkey') ||
       |            ' PRIMARY KEY (fid, ${info.cols.dtg.quoted})' || index_tablespace;
       |          -- creating a constraint allows it to be attached to the parent without any additional checks
       |          EXECUTE 'ALTER TABLE  ${info.schema.quoted}.' || quote_ident(partition_name) ||
       |            ' ADD CONSTRAINT ' || quote_ident(partition_name || '_constraint') ||
       |            ' CHECK ( ${info.cols.dtg.quoted} >= ' || quote_literal(partition_start) ||
       |            ' AND ${info.cols.dtg.quoted} < ' || quote_literal(partition_end) || ' )';
       |        END IF;
       |        RAISE INFO '% Done writing % rows to partition %', timeofday()::timestamp, unsorted_count, partition_name;
       |
       |        IF partition_parent = ${info.tables.spillPartitions.name.asLiteral} THEN
       |          -- store record of unsorted row counts which could negatively impact BRIN index scans
       |          INSERT INTO ${info.tables.sortQueue.name.qualified}(partition_name, unsorted_count, enqueued)
       |            VALUES (partition_name, unsorted_count, now());
       |          RAISE NOTICE 'Inserting % rows into spill partition %, queries may be impacted',
       |                unsorted_count, partition_name;
       |        END IF;
       |
       |        IF NOT pexists THEN
       |          EXECUTE 'ALTER TABLE ${info.schema.quoted}.' || quote_ident(partition_parent) ||
       |            ' ATTACH PARTITION ${info.schema.quoted}.' || quote_ident(partition_name) ||
       |            ' FOR VALUES FROM (' || quote_literal(partition_start) ||
       |            ') TO (' || quote_literal(partition_end) || ' );';
       |          -- now that we've attached the table we can drop the redundant constraint
       |          -- however, this requires ACCESS EXCLUSIVE - since constraints are only checked on inserts
       |          -- or updates, and partition tables are 'immutable' (only written to once), it shouldn't
       |          -- affect anything to leave it. note that for 'spill' tables, there may be some redundant checks
       |          -- EXECUTE 'ALTER TABLE ${info.schema.quoted}.' || quote_ident(partition_name) ||
       |          --  ' DROP CONSTRAINT ' || quote_ident(partition_name || '_constraint');
       |          RAISE NOTICE 'A partition has been created %', partition_name;
       |        END IF;
       |
       |        -- drop the tables that we've copied out
       |        EXECUTE 'DROP VIEW ' || quote_ident(partition_name || '_tmp_migrate');
       |        -- TODO this requires ACCESS EXCLUSIVE
       |        FOREACH write_ahead_partition IN ARRAY write_ahead_partitions LOOP
       |          EXECUTE 'DROP TABLE ' || write_ahead_partition;
       |          RAISE NOTICE 'A partition has been deleted %', write_ahead_partition;
       |        END LOOP;
       |
       |        -- mark the partition to be analyzed in a separate thread
       |        INSERT INTO ${info.tables.analyzeQueue.name.qualified}(partition_name, enqueued)
       |          VALUES (partition_name, now());
       |
       |        -- commit after each move, also releases the table locks
       |        COMMIT;
       |
       |      END LOOP;
       |    END;
       |  $$BODY$$;
       |""".stripMargin
  }
}
