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
 * Queues partitions older than the configured maximum for concurrent detachment and deletion
 */
object DropAgedOffPartitions extends SqlProcedure {

  override def name(info: TypeInfo): FunctionName = FunctionName(s"${info.typeIdentifier}_age_off")

  override protected def createStatements(info: TypeInfo): Seq[String] = Seq(proc(info))

  private def proc(info: TypeInfo): String = {
    s"""CREATE OR REPLACE PROCEDURE ${info.schema.quoted}.${name(info).quoted}(cur_time timestamp without time zone) LANGUAGE plpgsql AS
       |  $$BODY$$
       |    DECLARE
       |      num_partitions int;                          -- number of partitions to keep
       |      partition_size int;                          -- size of the partition, in hours
       |      main_cutoff timestamp without time zone;     -- max age of the records for main tables
       |      partition_start timestamp without time zone; -- start bounds for the partition we're writing
       |    BEGIN
       |      SELECT value::int FROM ${info.schema.quoted}.${UserDataTable.Name.quoted}
       |        WHERE key = ${literal(SftUserData.MaxPartitions.key)}
       |          AND type_name = ${literal(info.typeName)}
       |        INTO num_partitions;
       |      IF FOUND THEN
       |        SELECT COALESCE(
       |          (SELECT value::int FROM ${info.schema.quoted}.${UserDataTable.Name.quoted}
       |            WHERE type_name = ${literal(info.typeName)} AND key = ${literal(SftUserData.IntervalHours.key)}),
       |          ${SftUserData.IntervalHours.default})
       |          INTO partition_size;
       |        main_cutoff := ${info.schema.quoted}.truncate_to_partition(cur_time, partition_size) - make_interval(hours => partition_size);
       |        -- remove any partitions that have aged out
       |        partition_start := main_cutoff - (make_interval(hours => partition_size) * num_partitions);
       |        INSERT INTO ${info.tables.ageOffQueue.name.qualified}
       |          (source_partition, partition_oid, parent_oid, enqueued)
       |          SELECT quote_ident(namespace.nspname) || '.' || quote_ident(partition.relname),
       |              partition.oid, inherits.inhparent, now()
       |            FROM pg_catalog.pg_inherits inherits
       |            JOIN pg_catalog.pg_class partition ON partition.oid = inherits.inhrelid
       |            JOIN pg_catalog.pg_namespace namespace ON namespace.oid = partition.relnamespace
       |            WHERE (inherits.inhparent = ${info.tables.mainPartitions.name.asRegclass}
       |              AND partition.relname <= ${literal(info.tables.mainPartitions.name.raw + "_")} || to_char(partition_start, 'YYYY_MM_DD_HH24'))
       |            OR (inherits.inhparent = ${info.tables.spillPartitions.name.asRegclass}
       |              AND partition.relname <= ${literal(info.tables.spillPartitions.name.raw + "_")} || to_char(partition_start, 'YYYY_MM_DD_HH24'))
       |          ON CONFLICT (source_partition) DO NOTHING;
       |      END IF;
       |      CALL ${info.schema.quoted}.${CleanAgedOffPartitions.name(info).quoted}();
       |    END;
       |  $$BODY$$;
       |""".stripMargin
  }
}
