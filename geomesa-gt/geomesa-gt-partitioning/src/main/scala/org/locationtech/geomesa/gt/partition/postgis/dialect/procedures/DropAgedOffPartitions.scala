/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect
package procedures

import org.locationtech.geomesa.gt.partition.postgis.dialect.PartitionedPostgisDialect.SftUserData
import org.locationtech.geomesa.gt.partition.postgis.dialect.tables.UserDataTable

/**
 * Drops any partitions older than the configured maximum
 */
object DropAgedOffPartitions extends SqlProcedure {

  override def name(info: TypeInfo): FunctionName = FunctionName(s"${info.typeName}_age_off")

  override protected def createStatements(info: TypeInfo): Seq[String] = Seq(proc(info))

  private def proc(info: TypeInfo): String = {
    s"""CREATE OR REPLACE PROCEDURE ${name(info).quoted}(cur_time timestamp without time zone) LANGUAGE plpgsql AS
       |  $$BODY$$
       |    DECLARE
       |      num_partitions int;                          -- number of partitions to keep
       |      partition_size int;                          -- size of the partition, in hours
       |      main_cutoff timestamp without time zone;     -- max age of the records for main tables
       |      partition_start timestamp without time zone; -- start bounds for the partition we're writing
       |      partition_name regclass;                     -- partition table name
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
       |        main_cutoff := truncate_to_partition(cur_time, partition_size) - make_interval(hours => partition_size);
       |        -- remove any partitions that have aged out
       |        partition_start := main_cutoff - (make_interval(hours => partition_size) * num_partitions);
       |        FOR partition_name IN
       |          SELECT relid
       |            FROM pg_partition_tree(${info.tables.mainPartitions.name.asRegclass})
       |            WHERE parentrelid IS NOT NULL
       |            AND (SELECT relname FROM pg_class WHERE oid = relid) <= ${literal(info.tables.mainPartitions.name.raw + "_")} || to_char(partition_start, 'YYYY_MM_DD_HH24')
       |          UNION ALL
       |            SELECT relid
       |              FROM pg_partition_tree(${info.tables.spillPartitions.name.asRegclass})
       |              WHERE parentrelid IS NOT NULL
       |              AND (SELECT relname FROM pg_class WHERE oid = relid) <= ${literal(info.tables.spillPartitions.name.raw + "_")} || to_char(partition_start, 'YYYY_MM_DD_HH24')
       |        LOOP
       |          RAISE NOTICE 'Aging off partition %', partition_name;
       |          IF EXISTS(SELECT FROM pg_class WHERE oid = partition_name) THEN
       |            -- cast to text will handle quoting the table name appropriately
       |            EXECUTE 'DROP TABLE IF EXISTS ${info.schema.quoted}.' || partition_name::text;
       |            RAISE NOTICE 'A partition has been deleted %', partition_name;
       |          END IF;
       |        END LOOP;
       |      END IF;
       |    END;
       |  $$BODY$$;
       |""".stripMargin
  }
}
