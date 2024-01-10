/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect
package functions

import org.locationtech.geomesa.gt.partition.postgis.dialect.procedures.{AnalyzePartitions, PartitionMaintenance, RollWriteAheadLog}

/**
 * Deletes history older than 7 days
 */
object LogCleaner extends LogCleaner with AdvisoryLock {
  override protected val lockId: Long = 6743164310736814350L
}

class LogCleaner extends SqlFunction with CronSchedule {

  override def name(info: TypeInfo): FunctionName = FunctionName("cron_log_cleaner")

  override def jobName(info: TypeInfo): SqlLiteral = SqlLiteral(s"${info.typeName}-cron-log-cleaner")

  override protected def createStatements(info: TypeInfo): Seq[String] =
    Seq(function()) ++ super.createStatements(info)

  override protected def dropStatements(info: TypeInfo): Seq[String] = Seq.empty // function is shared between types

  override protected def schedule(info: TypeInfo): SqlLiteral = SqlLiteral("30 1 * * *") // 01:30 every day

  override protected def invocation(info: TypeInfo): SqlLiteral =
    SqlLiteral(s"SELECT cron_log_cleaner(${info.tables.view.name.asLiteral}, INTERVAL '7 DAYS')")

  private def function(): String = {
    val info = TypeInfo(SchemaName("", ""), "", null, null, null)
    val maintenanceSuffix = PartitionMaintenance.jobName(info).quoted
    val rollSuffix = RollWriteAheadLog.jobName(info).quoted
    val analyzeSuffix = AnalyzePartitions.jobName(info).quoted
    val logsSuffix = jobName(info).quoted

    s"""CREATE OR REPLACE FUNCTION cron_log_cleaner(name text, tokeep interval) RETURNS void LANGUAGE plpgsql AS
       |  $$BODY$$
       |    DECLARE
       |      maintenance_name text;
       |      roll_name text;
       |      analyze_name text;
       |      logs_name text;
       |    BEGIN
       |      maintenance_name := name || $maintenanceSuffix;
       |      roll_name := name || $rollSuffix;
       |      analyze_name := name || $analyzeSuffix;
       |      logs_name := name || $logsSuffix;
       |      DELETE FROM cron.job_run_details
       |        WHERE end_time < now() - tokeep
       |        AND jobid IN (
       |          SELECT jobid FROM cron.job
       |            WHERE jobname IN (maintenance_name, roll_name, analyze_name, logs_name)
       |        );
       |    END;
       |  $$BODY$$;""".stripMargin
  }
}
