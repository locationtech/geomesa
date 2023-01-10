/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect
package procedures

import org.locationtech.geomesa.gt.partition.postgis.dialect.tables.UserDataTable

/**
 * Drops any partitions older than the configured maximum
 */
object DropAgedOffPartitions extends SqlProcedure {

  override def name(info: TypeInfo): FunctionName = FunctionName(s"${info.typeName}_age_off")

  override protected def createStatements(info: TypeInfo): Seq[String] = Seq(proc(info))

  private def proc(info: TypeInfo): String = {
    val hours = info.partitions.hoursPerPartition
    val mainPartitions = info.tables.mainPartitions
    val spillPartitions = info.tables.spillPartitions
    s"""CREATE OR REPLACE PROCEDURE ${name(info).quoted}(cur_time timestamp without time zone) LANGUAGE plpgsql AS
       |  $$BODY$$
       |    DECLARE
       |      num_partitions int;                          -- number of partitions to keep
       |      main_cutoff timestamp without time zone;     -- max age of the records for main tables
       |      partition_start timestamp without time zone; -- start bounds for the partition we're writing
       |      partition_name regclass;                     -- partition table name
       |    BEGIN
       |      SELECT value::int FROM ${info.schema.quoted}.${UserDataTable.Name.quoted}
       |        WHERE key = ${literal(PartitionedPostgisDialect.Config.MaxPartitions)}
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9bd39d5310 (GEOMESA-3260 Postgis - fix age-off bug (#2958))
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
       |          AND type_name = ${literal(info.typeName)}
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
       |          AND type_name = ${literal(info.typeName)}
>>>>>>> 5c8e27c70f (GEOMESA-3260 Postgis - fix age-off bug (#2958))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
       |          AND type_name = ${literal(info.typeName)}
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
=======
       |          AND type_name = ${literal(info.typeName)}
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9bd39d5310 (GEOMESA-3260 Postgis - fix age-off bug (#2958))
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
       |          AND type_name = ${literal(info.typeName)}
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
       |        INTO num_partitions;
       |      IF FOUND THEN
       |        main_cutoff := truncate_to_partition(cur_time, $hours) - INTERVAL '$hours HOURS';
       |        -- remove any partitions that have aged out
       |        partition_start := main_cutoff - (INTERVAL '$hours HOURS' * num_partitions);
       |        FOR partition_name IN
       |          SELECT relid
       |            FROM pg_partition_tree(${mainPartitions.name.asRegclass})
       |            WHERE parentrelid IS NOT NULL
       |            AND (SELECT relname FROM pg_class WHERE oid = relid) <= ${literal(mainPartitions.name.raw + "_")} || to_char(partition_start, 'YYYY_MM_DD_HH24')
       |          UNION ALL
       |            SELECT relid
       |              FROM pg_partition_tree(${spillPartitions.name.asRegclass})
       |              WHERE parentrelid IS NOT NULL
       |              AND (SELECT relname FROM pg_class WHERE oid = relid) <= ${literal(spillPartitions.name.raw + "_")} || to_char(partition_start, 'YYYY_MM_DD_HH24')
       |        LOOP
                  RAISE NOTICE 'Aging off partition %', partition_name;
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
