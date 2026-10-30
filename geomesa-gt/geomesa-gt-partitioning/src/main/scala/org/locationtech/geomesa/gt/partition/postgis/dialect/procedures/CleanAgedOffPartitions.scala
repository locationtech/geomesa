/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect
package procedures

/**
 * Schedules concurrent detachments, recovers interrupted detachments, and drops detached expired partitions
 */
object CleanAgedOffPartitions extends SqlProcedure with CronSchedule {

  override def name(info: TypeInfo): FunctionName = FunctionName(s"${info.typeIdentifier}_clean_age_off")

  override def jobName(info: TypeInfo): SqlLiteral = SqlLiteral(s"${info.typeIdentifier}-clean-age-off")

  def detachJobName(info: TypeInfo, parent: TableConfig): SqlLiteral =
    SqlLiteral(s"${info.typeIdentifier}-age-off-${parent.name.raw}")

  override protected def schedule(info: TypeInfo): SqlLiteral = SqlLiteral("* * * * *")

  override protected def invocation(info: TypeInfo): SqlLiteral =
    SqlLiteral(s"CALL ${info.schema.quoted}.${name(info).quoted}()")

  override protected def createStatements(info: TypeInfo): Seq[String] =
    Seq(proc(info)) ++ super.createStatements(info)

  override protected def dropStatements(info: TypeInfo): Seq[String] = {
    val jobs = Seq(info.tables.mainPartitions, info.tables.spillPartitions)
        .map(parent => unscheduleSql(detachJobName(info, parent).quoted)).mkString("\n")
    Seq(s"DO $$$$ BEGIN $jobs END$$$$;") ++ super.dropStatements(info)
  }

  private def proc(info: TypeInfo): String = {
    val scheduleParents = Seq(info.tables.mainPartitions, info.tables.spillPartitions).map { parent =>
      val job = detachJobName(info, parent).quoted
      s"""SELECT queue.source_partition, inherits.inhdetachpending
         |  INTO partition_name, detach_pending
         |  FROM ${info.tables.ageOffQueue.name.qualified} queue
         |  JOIN pg_catalog.pg_inherits inherits ON inherits.inhrelid = queue.partition_oid
         |    AND inherits.inhparent = queue.parent_oid
         |  WHERE queue.parent_oid = ${parent.name.asRegclass}
         |  -- Only one detach may be pending per parent; finish it before starting another.
         |  ORDER BY inherits.inhdetachpending DESC, queue.enqueued, queue.source_partition
         |  LIMIT 1;
         |IF FOUND THEN
         |  detach_invocation := ${literal(s"ALTER TABLE ${parent.name.qualified} DETACH PARTITION ")} || partition_name ||
         |    CASE WHEN detach_pending THEN ' FINALIZE' ELSE ' CONCURRENTLY' END;
         |  PERFORM cron.schedule($job, '*/5 * * * *', detach_invocation);
         |ELSIF EXISTS (SELECT FROM cron.job WHERE jobname = $job) THEN
         |  PERFORM cron.unschedule($job);
         |END IF;""".stripMargin
    }.mkString("\n")
    s"""CREATE OR REPLACE PROCEDURE ${info.schema.quoted}.${name(info).quoted}() LANGUAGE plpgsql AS
       |  $$BODY$$
       |    DECLARE
       |      partition_name text;
       |      detach_pending boolean;
       |      detach_invocation text;
       |    BEGIN
       |      FOR partition_name IN
       |        SELECT queue.source_partition FROM ${info.tables.ageOffQueue.name.qualified} queue
       |        WHERE NOT EXISTS (SELECT FROM pg_catalog.pg_inherits WHERE inhrelid = queue.partition_oid)
       |        ORDER BY queue.source_partition
       |      LOOP
       |        EXECUTE 'DROP TABLE IF EXISTS ' || partition_name;
       |        DELETE FROM ${info.tables.ageOffQueue.name.qualified} WHERE source_partition = partition_name;
       |      END LOOP;
       |$scheduleParents
       |    END;
       |  $$BODY$$;
       |""".stripMargin
  }
}
