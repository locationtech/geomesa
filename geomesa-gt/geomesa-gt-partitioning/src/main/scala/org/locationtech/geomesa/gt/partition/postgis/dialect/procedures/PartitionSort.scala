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
 * Re-sorts a partition table. Can be used for maintenance if time-latent data comes in and degrades performance
 * of the BRIN index
 */
object PartitionSort extends SqlProcedure {

  override def name(info: TypeInfo): String = s"${info.name}_partition_sort"

  override protected def createStatements(info: TypeInfo): Seq[String] = Seq(proc(info))

  private def proc(info: TypeInfo): String = {
    val hours = info.partitions.hoursPerPartition
    s"""CREATE OR REPLACE PROCEDURE "${name(info)}"(for_date timestamp without time zone) LANGUAGE plpgsql AS
       |  $$BODY$$
       |    DECLARE
       |      partition_names text[];
       |      partition_name text;
       |    BEGIN
       |      IF for_date IS NOT NULL THEN
       |        partition_names := ARRAY[
       |          '${info.tables.mainPartitions.name.raw}' || '_' || to_char(truncate_to_partition(for_date, $hours), 'YYYY_MM_DD_HH24')
       |        ];
       |      ELSE
       |        partition_names := Array(
       |          SELECT relid
       |            FROM pg_partition_tree('${info.tables.schema}.${info.tables.mainPartitions.name.raw}'::regclass)
       |            WHERE parentrelid IS NOT NULL
       |            ORDER BY relid
       |        );
       |        -- start a new transaction to ensure our lock is correct
       |        COMMIT;
       |      END IF;
       |
       |      FOREACH partition_name IN ARRAY partition_names LOOP
       |        RAISE INFO '% Sorting partition table %', timeofday()::timestamp, partition_name;
       |        LOCK TABLE ONLY ${info.tables.mainPartitions.name.full} IN SHARE UPDATE EXCLUSIVE MODE;
       |        EXECUTE 'LOCK TABLE "${info.schema}".' || quote_ident(partition_name) ||
       |          ' IN SHARE UPDATE EXCLUSIVE MODE';
       |        EXECUTE 'CREATE TEMP TABLE ' || quote_ident(partition_name || '_tmp_sort') || ' ON COMMIT DROP' ||
       |          ' AS SELECT * FROM "${info.schema}".' || quote_ident(partition_name);
       |        EXECUTE 'ANALYZE ' || quote_ident(partition_name || '_tmp_sort');
       |        EXECUTE 'TRUNCATE "${info.schema}".' || quote_ident(partition_name);
       |        EXECUTE 'INSERT INTO "${info.schema}".' || quote_ident(partition_name) ||
       |          ' (SELECT * FROM ' || quote_ident(partition_name || '_tmp_sort') ||
       |          ' ORDER BY st_geohash(${info.cols.geom.name}), ${info.cols.dtg.name})';
       |        -- mark the partition to be analyzed in a separate thread
       |        INSERT INTO ${info.tables.analyzeQueue.name.full}(partition_name, enqueued)
       |          VALUES (partition_name, now());
       |        COMMIT;
       |      END LOOP;
       |
       |    END;
       |  $$BODY$$;
       |""".stripMargin
  }
}
