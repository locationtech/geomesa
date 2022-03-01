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

/**
 * Partitions the write ahead table into the recent and/or main partition tables
 */
object PartitionWriteAheadLog extends SqlProcedure {

  override def name(info: TypeInfo): FunctionName = FunctionName(s"${info.typeName}_partition_wa")

  override protected def createStatements(info: TypeInfo): Seq[String] = Seq(proc(info))

  private def proc(info: TypeInfo): String = {
    val hours = info.partitions.hoursPerPartition
    val writeAhead = info.tables.writeAhead
    val writeAheadPartitions = info.tables.writeAheadPartitions
    val mainPartitions = info.tables.mainPartitions
    val partitionsTable = s"${info.schema.quoted}.${PartitionTablespacesTable.Name.quoted}"
    val dtgCol = info.cols.dtg.quoted
    val geomCol = info.cols.geom.quoted

    s"""CREATE OR REPLACE PROCEDURE ${name(info).quoted}(cur_time timestamp without time zone) LANGUAGE plpgsql AS
       |  $$BODY$$
       |    DECLARE
       |      min_dtg timestamp without time zone;         -- min date in our partitioned tables
       |      main_cutoff timestamp without time zone;     -- max age of the records for main tables
       |      write_ahead record;
       |      partition_start timestamp without time zone; -- start bounds for the partition we're writing
       |      partition_end timestamp without time zone;   -- end bounds for the partition we're writing
       |      partition_name text;                         -- partition table name
       |      partition_parent text;                       -- partition parent table name
       |      partition_name_format text;                  -- date format for partition names
       |      partition_tablespace text;                   -- partition tablespace
       |      pexists boolean;                             -- table exists check
       |      unsorted_count bigint;
       |    BEGIN
       |      -- constants
       |      main_cutoff := truncate_to_partition(cur_time, $hours) - INTERVAL '$hours HOURS';
       |
       |      -- check for write ahead partitions and move the data into the time partitioned tables
       |      FOR write_ahead IN
       |        SELECT pg_class.relname AS name
       |          FROM pg_catalog.pg_inherits
       |          INNER JOIN pg_catalog.pg_class ON (pg_inherits.inhrelid = pg_class.oid)
       |          INNER JOIN pg_catalog.pg_namespace ON (pg_class.relnamespace = pg_namespace.oid)
       |          WHERE inhparent = ${writeAhead.name.asLiteral}::regclass
       |          AND relname != ${WriteAheadTable.writesPartition(info).asLiteral}
       |          ORDER BY name
       |      LOOP
       |
       |        RAISE INFO '% Checking write ahead table %', timeofday()::timestamp, write_ahead.name;
       |        -- get a lock on the table - this mode won't prevent reads but will prevent writes
       |        -- (there shouldn't be any writes though) and will synchronize this method
       |        LOCK TABLE ONLY ${mainPartitions.name.qualified} IN SHARE UPDATE EXCLUSIVE MODE;
       |        EXECUTE 'LOCK TABLE ${info.schema.quoted}.' || quote_ident(write_ahead.name) ||
       |          ' IN SHARE UPDATE EXCLUSIVE MODE';
       |        RAISE INFO '% Locked write ahead table % for migration', timeofday()::timestamp, write_ahead.name;
       |
       |        -- wait until the table doesn't contain any recent records
       |        EXECUTE 'SELECT EXISTS(SELECT 1 FROM ${info.schema.quoted}.' || quote_ident(write_ahead.name) ||
       |          ' WHERE $dtgCol >= ' || quote_literal(truncate_to_ten_minutes(cur_time)) || ')'
       |          INTO pexists;
       |
       |        IF pexists THEN
       |          -- should only happen if data is inserted with timestamps from the future
       |          RAISE NOTICE '% Skipping write ahead table % due to min date', timeofday()::timestamp, write_ahead.name;
       |        ELSE
       |          partition_end := '-infinity'::timestamp without time zone;
       |          LOOP
       |            -- find the range of dates in the write ahead partition
       |            EXECUTE 'SELECT min($dtgCol) FROM ${info.schema.quoted}.' || quote_ident(write_ahead.name) ||
       |              ' WHERE $dtgCol >= ' || quote_literal(partition_end) INTO min_dtg;
       |            EXIT WHEN min_dtg IS NULL;
       |
       |            -- calculate the partition bounds for the min date
       |            IF min_dtg < main_cutoff THEN
       |              partition_start := truncate_to_partition(min_dtg, $hours);
       |              partition_end := partition_start + INTERVAL '$hours HOURS';
       |              partition_parent := ${mainPartitions.name.asLiteral};
       |              partition_name_format := 'YYYY_MM_DD_HH24';
       |              SELECT table_space INTO partition_tablespace FROM $partitionsTable
       |                WHERE type_name = ${literal(info.typeName)} AND table_type = ${PartitionedTableSuffix.quoted};
       |              IF partition_tablespace IS NULL THEN
       |                partition_tablespace := '${mainPartitions.storage.opts}';
       |              ELSE
       |                partition_tablespace := '${mainPartitions.storage.opts} TABLESPACE ' ||
       |                  quote_ident(partition_tablespace);
       |              END IF;
       |            ELSE
       |              partition_start := truncate_to_ten_minutes(min_dtg);
       |              partition_end := partition_start + INTERVAL '10 MINUTES';
       |              partition_parent := ${writeAheadPartitions.name.asLiteral};
       |              partition_name_format := 'YYYY_MM_DD_HH24_MI';
       |              SELECT table_space INTO partition_tablespace FROM $partitionsTable
       |                WHERE type_name = ${literal(info.typeName)} AND table_type = ${PartitionedWriteAheadTableSuffix.quoted};
       |              IF partition_tablespace IS NULL THEN
       |                partition_tablespace := '${writeAheadPartitions.storage.opts}';
       |              ELSE
       |                partition_tablespace := '${writeAheadPartitions.storage.opts} TABLESPACE ' ||
       |                  quote_ident(partition_tablespace);
       |              END IF;
       |            END IF;
       |
       |            partition_name := partition_parent || '_' || to_char(partition_start, partition_name_format);
       |
       |            RAISE INFO '% Writing to partition %', timeofday()::timestamp, partition_name;
       |
       |            SELECT EXISTS(SELECT FROM pg_tables WHERE schemaname = ${info.schema.asLiteral} AND tablename = partition_name)
       |              INTO pexists;
       |
       |            -- create the partition table if it doesn't exist
       |            -- note: normally the partition will not exist unless time-latent data was inserted
       |            -- we create it unattached so that it doesn't lock the _recent table on insert
       |            -- then we attach it after inserting the rows
       |            -- since this is all within a transaction it should all happen "at once"
       |            -- see https://www.postgresql.org/docs/13/ddl-partitioning.html#DDL-PARTITIONING-DECLARATIVE-MAINTENANCE
       |            IF NOT pexists THEN
       |              RAISE INFO '% Creating partition % (unattached)', timeofday()::timestamp, partition_name;
       |              -- upper bounds are exclusive
       |              EXECUTE 'CREATE TABLE ${info.schema.quoted}.' || quote_ident(partition_name) ||
       |                ' (LIKE ${info.schema.quoted}.' || quote_ident(partition_parent) ||
       |                ' INCLUDING DEFAULTS INCLUDING CONSTRAINTS)' || partition_tablespace;
       |              -- creating a constraint allows it to be attached to the parent without any additional checks
       |              EXECUTE 'ALTER TABLE  ${info.schema.quoted}.' || quote_ident(partition_name) ||
       |                ' ADD CONSTRAINT ' || quote_ident(partition_name || '_constraint') ||
       |                ' CHECK ( $dtgCol >= ' || quote_literal(partition_start) ||
       |                ' AND $dtgCol < ' || quote_literal(partition_end) || ' );';
       |            END IF;
       |
       |            -- copy rows from write ahead table to partition table
       |            RAISE INFO '% Copying rows to partition %', timeofday()::timestamp, partition_name;
       |            EXECUTE 'INSERT INTO ${info.schema.quoted}.' || quote_ident(partition_name) ||
       |              ' SELECT * FROM ' || quote_ident(write_ahead.name) ||
       |              '   WHERE $dtgCol >= ' || quote_literal(partition_start) ||
       |              '     AND $dtgCol < ' || quote_literal(partition_end) ||
       |              '   ORDER BY st_geohash($geomCol), $dtgCol' ||
       |              '   ON CONFLICT DO NOTHING';
       |            RAISE INFO '% Done copying rows to partition %', timeofday()::timestamp, partition_name;
       |
       |            -- attach the partition table to the parent
       |            IF NOT pexists THEN
       |              RAISE INFO '% Attaching partition % to parent', timeofday()::timestamp, partition_name;
       |              EXECUTE 'ALTER TABLE ${info.schema.quoted}.' || quote_ident(partition_parent) ||
       |                ' ATTACH PARTITION ${info.schema.quoted}.' || quote_ident(partition_name) ||
       |                ' FOR VALUES FROM (' || quote_literal(partition_start) ||
       |                ') TO (' || quote_literal(partition_end) || ' );';
       |              -- once the table is attached we can drop the redundant constraint
       |              EXECUTE 'ALTER TABLE ${info.schema.quoted}.' || quote_ident(partition_name) ||
       |                ' DROP CONSTRAINT ' || quote_ident(partition_name || '_constraint');
       |              RAISE NOTICE 'A partition has been created %', partition_name;
       |            ELSIF partition_parent = ${mainPartitions.name.asLiteral} THEN
       |              -- store record of unsorted row counts which could negatively impact BRIN index scans
       |              GET DIAGNOSTICS unsorted_count := ROW_COUNT;
       |              INSERT INTO ${info.tables.sortQueue.name.qualified}(partition_name, unsorted_count, enqueued)
       |                VALUES (partition_name, unsorted_count, now());
       |              RAISE NOTICE 'Inserting % rows into existing partition %, queries may be impacted',
       |                unsorted_count, partition_name;
       |            END IF;
       |
       |            -- mark the partition to be analyzed in a separate thread
       |            INSERT INTO ${info.tables.analyzeQueue.name.qualified}(partition_name, enqueued)
       |              VALUES (partition_name, now());
       |          END LOOP;
       |
       |          RAISE INFO '% Dropping write ahead table %', timeofday()::timestamp, write_ahead.name;
       |          EXECUTE 'DROP TABLE ' || quote_ident(write_ahead.name);
       |
       |        END IF;
       |
       |        COMMIT; -- releases the lock
       |      END LOOP;
       |    END;
       |  $$BODY$$;
       |""".stripMargin
  }
}
