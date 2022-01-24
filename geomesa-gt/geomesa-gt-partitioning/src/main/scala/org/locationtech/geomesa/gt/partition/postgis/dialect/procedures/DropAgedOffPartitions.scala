/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect
package procedures

/**
 * Drops any partitions older than the configured maximum
 */
object DropAgedOffPartitions extends SqlProcedure {

  override def name(info: TypeInfo): String = s"${info.name}_age_off"

  override protected def createStatements(info: TypeInfo): Seq[String] = Seq(proc(info))

  private def proc(info: TypeInfo): String = {
    val hours = info.partitions.hoursPerPartition
    val mainPartitions = info.tables.mainPartitions
    val max = info.partitions.maxPartitions.getOrElse(throw new IllegalStateException("No configured max partitions"))

    s"""CREATE OR REPLACE PROCEDURE "${name(info)}"(cur_time timestamp without time zone) LANGUAGE plpgsql AS
       |  $$BODY$$
       |    DECLARE
       |      main_cutoff timestamp without time zone;     -- max age of the records for main tables
       |      partition_start timestamp without time zone; -- start bounds for the partition we're writing
       |      partition_name text;                         -- partition table name
       |    BEGIN
       |      -- constants
       |      main_cutoff := truncate_to_partition(cur_time, $hours) - INTERVAL '$hours HOURS';
       |
       |      -- remove any partitions that have aged out
       |      partition_start := main_cutoff - INTERVAL '${hours * max} HOURS'
       |      FOR partition_name IN
       |        SELECT relid
       |          FROM pg_partition_tree('${info.schema}.${mainPartitions.name.raw}'::regclass)
       |          WHERE parentrelid IS NOT NULL
       |          AND child <= '${mainPartitions.name.raw}_' || to_char(partition_start, 'YYYY_MM_DD_HH24')
       |      LOOP
       |        IF EXISTS(SELECT FROM pg_tables WHERE schemaname = '${info.schema}' AND tablename = partition_name) THEN
       |          EXECUTE 'DROP TABLE IF EXISTS "${info.schema}".' || quote_ident(partition_name);
       |          RAISE NOTICE 'A partition has been deleted %', partition_name;
       |        END IF;
       |      END LOOP;
       |    END;
       |  $$BODY$$;
       |""".stripMargin
  }
}
