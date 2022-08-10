/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
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
    val hours = info.partitions.hoursPerPartition
    val partitionsTable = s"${info.schema.quoted}.${PartitionTablespacesTable.Name.quoted}"
    s"""CREATE OR REPLACE PROCEDURE ${name(info).quoted}(for_date timestamp without time zone) LANGUAGE plpgsql AS
       |  $$BODY$$
       |    DECLARE
       |      partition_names text[];
       |      partition_name text;
       |      spill_partition text;
       |      min_dtg timestamp without time zone;         -- min date in our partitioned tables
       |      partition_start timestamp without time zone; -- start bounds for the partition we're writing
       |      partition_end timestamp without time zone;   -- end bounds for the partition we're writing
       |      partition_tablespace text;
       |      pexists boolean;                             -- table exists check
       |    BEGIN
       |      IF for_date IS NOT NULL THEN
       |        partition_names := ARRAY[
       |          ${info.tables.mainPartitions.name.asLiteral} || '_' || to_char(truncate_to_partition(for_date, $hours), 'YYYY_MM_DD_HH24')
       |        ];
       |      ELSE
       |        partition_names := Array(
       |          SELECT relid
       |            FROM pg_partition_tree(${info.tables.mainPartitions.name.asRegclass})
       |            WHERE parentrelid IS NOT NULL
       |            ORDER BY relid
       |        );
       |        -- start a new transaction to ensure our lock is correct
       |        COMMIT;
       |      END IF;
       |
       |      SELECT table_space INTO partition_tablespace FROM $partitionsTable
       |        WHERE type_name = ${literal(info.typeName)} AND table_type = ${PartitionedTableSuffix.quoted};
       |      IF partition_tablespace IS NULL THEN
       |        partition_tablespace := '';
       |      ELSE
       |        partition_tablespace := ' TABLESPACE ' || quote_ident(partition_tablespace);
       |      END IF;
       |
       |      FOREACH partition_name IN ARRAY partition_names LOOP
       |        RAISE INFO '% Compacting partition table %', timeofday()::timestamp, partition_name;
       |        LOCK TABLE ONLY ${info.tables.mainPartitions.name.qualified} IN SHARE UPDATE EXCLUSIVE MODE;
       |        -- lock the child table to prevent any inserts that would be lost
       |        EXECUTE 'LOCK TABLE ${info.schema.quoted}.' || quote_ident(partition_name) ||
       |          ' IN SHARE ROW EXCLUSIVE MODE';
       |        EXECUTE 'SELECT $dtgCol FROM ${info.schema.quoted}.' || quote_ident(partition_name) ||
       |          ' LIMIT 1' INTO min_dtg;
       |        partition_start := truncate_to_partition(min_dtg, $hours);
       |        partition_end := partition_start + INTERVAL '$hours HOURS';
       |        spill_partition := ${info.tables.spillPartitions.name.asLiteral} || '_' || to_char(partition_start, 'YYYY_MM_DD_HH24');
       |
       |        SELECT EXISTS(SELECT FROM pg_tables WHERE schemaname = ${info.schema.asLiteral} AND tablename = spill_partition)
       |          INTO pexists;
       |
       |        IF pexists THEN
       |          -- lock the child table to prevent any inserts that would be lost
       |          EXECUTE 'LOCK TABLE ${info.schema.quoted}.' || quote_ident(spill_partition) ||
       |            ' IN SHARE ROW EXCLUSIVE MODE';
       |        END IF;
       |
       |        -- this won't have any indices until after we populate it with data
       |        EXECUTE 'CREATE TABLE ${info.schema.quoted}.' || quote_ident(partition_name || '_tmp_sort') ||
       |          ' (LIKE ${info.tables.mainPartitions.name.qualified} INCLUDING DEFAULTS INCLUDING CONSTRAINTS)' ||
       |          partition_tablespace;
       |        -- creating a constraint allows it to be attached to the parent without any additional checks
       |        EXECUTE 'ALTER TABLE  ${info.schema.quoted}.' || quote_ident(partition_name || '_tmp_sort') ||
       |          ' ADD CONSTRAINT ' || quote_ident(partition_name || '_constraint') ||
       |          ' CHECK ( $dtgCol >= ' || quote_literal(partition_start) ||
       |          ' AND $dtgCol < ' || quote_literal(partition_end) || ' );';
       |
       |        IF pexists THEN
       |          EXECUTE 'INSERT INTO ${info.schema.quoted}.' || quote_ident(partition_name || '_tmp_sort') ||
       |            ' (SELECT * FROM (SELECT * FROM ${info.schema.quoted}.' || quote_ident(partition_name) ||
       |            ' UNION ALL SELECT * FROM ${info.schema.quoted}.' || quote_ident(spill_partition) ||
       |            ') results ORDER BY st_geohash(${info.cols.geom.quoted}), ${info.cols.dtg.quoted})';
       |        ELSE
       |          EXECUTE 'INSERT INTO ${info.schema.quoted}.' || quote_ident(partition_name || '_tmp_sort') ||
       |            ' (SELECT * FROM ${info.schema.quoted}.' || quote_ident(partition_name) ||
       |            ' ORDER BY st_geohash(${info.cols.geom.quoted}), ${info.cols.dtg.quoted})';
       |        END IF;
       |
       |        -- create indices before attaching to minimize time to attach, copied from PartitionTables code
       |        EXECUTE 'CREATE INDEX IF NOT EXISTS ' || quote_ident(partition_name || '_${info.cols.geom.raw}_tmp_sort') ||
       |          ' ON ${info.schema.quoted}.' || quote_ident(partition_name || '_tmp_sort') ||
       |          ' USING BRIN(${info.cols.geom.quoted})' || partition_tablespace;
       |        EXECUTE 'CREATE INDEX IF NOT EXISTS ' || quote_ident(partition_name || '_${info.cols.dtg.raw}_tmp_sort') ||
       |          ' ON ${info.schema.quoted}.' || quote_ident(partition_name || '_tmp_sort') ||
       |          ' (${info.cols.dtg.quoted})' || partition_tablespace;
       |${info.cols.indexed.map { col =>
    s"""        EXECUTE 'CREATE INDEX IF NOT EXISTS ' || quote_ident(partition_name || '_${col.raw}_tmp_sort') ||
       |          ' ON ${info.schema.quoted}.' || quote_ident(partition_name || '_tmp_sort') ||
       |          ' (${col.quoted})' || partition_tablespace;""".stripMargin}.mkString("\n")}
       |
       |        RAISE INFO '% Dropping old partition table (queries will be blocked) %', timeofday()::timestamp, partition_name;
       |        EXECUTE 'DROP TABLE IF EXISTS ${info.schema.quoted}.' || quote_ident(partition_name);
       |        EXECUTE 'DROP TABLE IF EXISTS ${info.schema.quoted}.' || quote_ident(spill_partition);
       |
       |        RAISE INFO '% Renaming newly sorted partition table %', timeofday()::timestamp, partition_name;
       |        EXECUTE 'ALTER TABLE ${info.schema.quoted}.' || quote_ident(partition_name || '_tmp_sort') ||
       |          ' RENAME TO ' || quote_ident(partition_name);
       |        EXECUTE 'ALTER INDEX ' || quote_ident(partition_name || '_${info.cols.geom.raw}_tmp_sort') ||
       |          ' RENAME TO ' || quote_ident(partition_name || '_${info.cols.geom.raw}');
       |        EXECUTE 'ALTER INDEX ' || quote_ident(partition_name || '_${info.cols.dtg.raw}_tmp_sort') ||
       |          ' RENAME TO ' || quote_ident(partition_name || '_${info.cols.dtg.raw}');
       |${info.cols.indexed.map { col =>
    s"""        EXECUTE 'ALTER INDEX ' || quote_ident(partition_name || '_${col.raw}_tmp_sort') ||
       |          ' RENAME TO ' || quote_ident(partition_name || '_${col.raw}');""".stripMargin}.mkString("\n")}
       |
       |        RAISE INFO '% Attaching newly sorted partition table %', timeofday()::timestamp, partition_name;
       |        EXECUTE 'ALTER TABLE ${info.tables.mainPartitions.name.qualified}' ||
       |          ' ATTACH PARTITION ${info.schema.quoted}.' || quote_ident(partition_name) ||
       |          ' FOR VALUES FROM (' || quote_literal(partition_start) || ') TO (' ||
       |          quote_literal(partition_end) || ' );';
       |        RAISE INFO '% Done compacting partition table %', timeofday()::timestamp, partition_name;
       |
       |        -- now that we've attached the table we can drop the redundant constraint
       |        RAISE INFO '% Dropping constraint for partition table %', timeofday()::timestamp, partition_name;
       |        EXECUTE 'ALTER TABLE ${info.schema.quoted}.' || quote_ident(partition_name) ||
       |            ' DROP CONSTRAINT ' || quote_ident(partition_name || '_constraint');
       |
       |        -- mark the partition to be analyzed in a separate thread
       |        INSERT INTO ${info.tables.analyzeQueue.name.qualified}(partition_name, enqueued)
       |          VALUES (partition_name, now());
       |
       |        -- commit to release our locks
       |        COMMIT;
       |      END LOOP;
       |
       |    END;
       |  $$BODY$$;
       |""".stripMargin
  }
}
