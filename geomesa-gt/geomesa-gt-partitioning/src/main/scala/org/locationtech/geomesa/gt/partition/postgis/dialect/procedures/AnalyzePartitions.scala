/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect
package procedures

/**
 * Runs 'analyze' on partitions that have been modified
 */
object AnalyzePartitions extends SqlProcedure with CronSchedule {

  override def name(info: TypeInfo): FunctionName = FunctionName(s"${info.typeName}_analyze_partitions")

  override def jobName(info: TypeInfo): SqlLiteral = SqlLiteral(s"${info.typeName}-analyze")

  override protected def createStatements(info: TypeInfo): Seq[String] =
    Seq(proc(info)) ++ super.createStatements(info)

  override protected def schedule(info: TypeInfo): SqlLiteral = SqlLiteral("* * * * *") // run every minute

  override protected def invocation(info: TypeInfo): SqlLiteral = SqlLiteral(s"CALL ${name(info).quoted}()")

  private def proc(info: TypeInfo): String = {
    s"""CREATE OR REPLACE PROCEDURE ${name(info).quoted}() LANGUAGE plpgsql AS
       |  $$BODY$$
       |    DECLARE
       |      cur_time timestamp without time zone;        -- current time
       |      to_analyze record;                           -- result
       |    BEGIN
       |      LOOP
       |        cur_time := now();
       |        SELECT * INTO to_analyze FROM ${info.tables.analyzeQueue.name.qualified}
       |          WHERE enqueued < cur_time
       |          ORDER BY enqueued ASC;
       |        EXIT WHEN to_analyze IS NULL;
       |        IF EXISTS(SELECT FROM pg_tables WHERE schemaname = ${info.schema.asLiteral} AND tablename = to_analyze.partition_name) THEN
       |          RAISE INFO '% Running analyze on partition table %', timeofday()::timestamp, to_analyze.partition_name;
       |          EXECUTE 'ANALYZE ${info.schema.quoted}.' || quote_ident(to_analyze.partition_name);
       |        ELSE
       |          RAISE INFO '% Skipping analyze on dropped partition table %', timeofday()::timestamp, to_analyze.partition_name;
       |        END IF;
       |        DELETE FROM ${info.tables.analyzeQueue.name.qualified}
       |          WHERE partition_name = to_analyze.partition_name AND enqueued < cur_time;
       |        COMMIT;
       |      END LOOP;
       |    END;
       |  $$BODY$$;
       |""".stripMargin
  }
}
