/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect
package procedures

/**
 * Removes migrated numeric write-ahead tables when readers permit it. Migration deletes the copied rows
 * through MVCC, so vacuum and slot reuse do not depend on this best-effort removal.
 */
object CleanWriteAheadRetirements extends SqlProcedure with CronSchedule {

  override def name(info: TypeInfo): FunctionName = FunctionName(s"${info.typeIdentifier}_clean_wa_retirements")

  override def jobName(info: TypeInfo): SqlLiteral = SqlLiteral(s"${info.typeIdentifier}-clean-wa-retirements")

  override protected def schedule(info: TypeInfo): SqlLiteral = SqlLiteral("* * * * *")

  override protected def invocation(info: TypeInfo): SqlLiteral =
    SqlLiteral(s"CALL ${info.schema.quoted}.${name(info).quoted}()")

  override protected def createStatements(info: TypeInfo): Seq[String] = Seq(proc(info)) ++ super.createStatements(info)

  private def proc(info: TypeInfo): String =
    s"""CREATE OR REPLACE PROCEDURE ${info.schema.quoted}.${name(info).quoted}() LANGUAGE plpgsql AS
       |  $$BODY$$
       |    DECLARE retired record;
       |    BEGIN
       |      FOR retired IN
       |        SELECT * FROM ${info.tables.writeAheadRetirements.name.qualified} ORDER BY source_partition
       |      LOOP
       |        BEGIN
       |          IF EXISTS (SELECT FROM pg_class WHERE oid = retired.partition_oid) THEN
       |            -- Match log rotation's lock order, and never queue behind a reader or another cleanup.
       |            LOCK TABLE ONLY ${info.tables.writeAhead.name.qualified} IN SHARE UPDATE EXCLUSIVE MODE NOWAIT;
       |            EXECUTE 'LOCK TABLE ONLY ' || retired.source_partition || ' IN ACCESS EXCLUSIVE MODE NOWAIT';
       |            -- Rotation can reuse an emptied table between enumeration and acquiring these locks.
       |            IF EXISTS (SELECT FROM ${info.tables.writeAheadRetirements.name.qualified}
       |                WHERE partition_oid = retired.partition_oid AND enqueued = retired.enqueued) THEN
       |              EXECUTE 'DROP TABLE ' || retired.source_partition;
       |            END IF;
       |          END IF;
       |          DELETE FROM ${info.tables.writeAheadRetirements.name.qualified}
       |            WHERE partition_oid = retired.partition_oid AND enqueued = retired.enqueued;
       |        EXCEPTION WHEN lock_not_available THEN
       |          RAISE DEBUG 'Deferring removal of numeric write-ahead table %', retired.source_partition;
       |        END;
       |        COMMIT;
       |      END LOOP;
       |    END;
       |  $$BODY$$;""".stripMargin
}
