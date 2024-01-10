/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect
package procedures

import scala.util.hashing.MurmurHash3

/**
 * High level procedure to manage data shuffling between write-ahead table and partitioned tables
 */
object PartitionMaintenance extends SqlProcedure with CronSchedule {

  override def name(info: TypeInfo): FunctionName = FunctionName(s"${info.typeName}_partition_maintenance")

  override def jobName(info: TypeInfo): SqlLiteral = SqlLiteral(s"${info.typeName}-part-maintenance")

  override protected def createStatements(info: TypeInfo): Seq[String] =
    Seq(proc(info)) ++ super.createStatements(info)

  override protected def schedule(info: TypeInfo): SqlLiteral = {
    val minute = info.partitions.cronMinute.getOrElse {
      // spread out the cron schedule so that all the feature types don't run at the exact same time
      // also don't run at same minute as roll-write-ahead (i.e. use 0-8)
      math.abs(MurmurHash3.stringHash(info.typeName) % 9)
    }
    val minutes = Seq(0, 10, 20, 30, 40, 50).map(_ + minute).mkString(",")
    SqlLiteral(s"$minutes * * * *")
  }

  override protected def invocation(info: TypeInfo): SqlLiteral = SqlLiteral(s"CALL ${name(info).quoted}()")

  private def proc(info: TypeInfo): String = {
    s"""CREATE OR REPLACE PROCEDURE ${name(info).quoted}() LANGUAGE plpgsql AS
       |  $$BODY$$
       |    DECLARE
       |      cur_time timestamp without time zone;        -- current time
       |    BEGIN
       |      -- constants
       |      cur_time := now();
       |      CALL ${PartitionWriteAheadLog.name(info).quoted}(cur_time);
       |      CALL ${MergeWriteAheadPartitions.name(info).quoted}(cur_time);
       |      CALL ${DropAgedOffPartitions.name(info).quoted}(cur_time);
       |    END;
       |  $$BODY$$;
       |""".stripMargin
  }
}
