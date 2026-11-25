/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect
package procedures

import org.locationtech.geomesa.gt.partition.postgis.dialect.PartitionedPostgisDialect.SftUserData
import org.locationtech.geomesa.gt.partition.postgis.dialect.tables.{MainView, UserDataTable}
import org.locationtech.geomesa.gt.partition.postgis.dialect.triggers.{DeleteTrigger, UpdateTrigger}

/**
 * Drops the previous generation of a feature type, once all of its data has aged off.
 *
 * The main view and triggers for the current generation depend on the view for the previous generation, so they
 * are re-created without it. Replacing the view requires an exclusive lock on it - if the lock can't be acquired
 * quickly (due to queries against the view), then we give up and try again the next time the procedure is run.
 *
 * If the previous generation can't be checked or dropped for any other reason (for example, if something outside of
 * our control depends on its main view, or one of its tables has been modified), then we log a warning and try again
 * the next time the procedure is run, so that partition maintenance for the current generation isn't affected.
 *
 * Writes to the previous generation are blocked while we verify that it is empty and drop it, so that we don't drop
 * anything that is written (or committed) after we check.
 *
 * Once the previous generation has been retired, the procedure drops itself, and removes the call to itself from
 * partition maintenance. The previous generation is only identified by name, so otherwise the procedure would go
 * on to drop anything that was subsequently created with the same name.
 *
 * Age-off is configured per generation, so until the previous generation is retired, the procedure also keeps its
 * max partitions in sync with the current generation, in case the configuration is changed directly in the user
 * data table.
 */
object RetirePreviousGeneration extends Sql {

  def name(info: TypeInfo): FunctionName = FunctionName(s"${info.typeIdentifier}_retire_previous")

  override def create(info: TypeInfo)(implicit ex: ExecutionContext): Unit = {
    info.previous match {
      case None => drop(info)
      case Some(prev) => ex.execute(proc(info, prev, statements(info, prev)))
    }
  }

  override def dropSql(info: TypeInfo): Seq[String] =
    Seq(s"DROP PROCEDURE IF EXISTS ${info.schema.quoted}.${name(info).quoted};")

  /**
   * Gets the statements required to retire the previous generation.
   *
   * Note that the statements are executed later, by the procedure, so they are generated without reference to the
   * connection that is creating the procedure. They also don't take the advisory locks that are used when dropping
   * a schema through the data store - the locks are there to synchronize creating the shared tables, and the
   * procedure only deletes rows from them.
   *
   * @param info type info
   * @param prev previous generation type info
   * @return
   */
  private def statements(info: TypeInfo, prev: TypeInfo): Seq[String] = {
    // re-create the things that reference the previous generation
    val current = info.copy(previous = None)
    val create = Seq(MainView, UpdateTrigger, DeleteTrigger).flatMap(_.createSql(current))
    // drop the previous generation - this is the same as removing a schema through the data store
    val drop = PartitionedPostgisDialect.dropSql(prev)
    // the previous generation only needs to be retired once, so remove this procedure and the call to it - otherwise
    // it would also retire anything that is subsequently created with the same name as the previous generation
    // note: we only replace the maintenance procedure, we don't need to re-schedule the cron job that invokes it
    create ++ drop ++ Seq(PartitionMaintenance.proc(current)) ++ dropSql(info)
  }

  private def proc(info: TypeInfo, prev: TypeInfo, statements: Seq[String]): String = {
    val userData = s"${info.schema.quoted}.${UserDataTable.Name.quoted}"
    val tables = Seq(prev.tables.writeAhead, prev.tables.writeAheadPartitions, prev.tables.mainPartitions, prev.tables.spillPartitions)
    val maxPartitions = literal(SftUserData.MaxPartitions.key)
    val notEmpty = tables.map(t => s"EXISTS (SELECT FROM ${t.name.qualified})").mkString("\n          OR ")
    // note: the statements contain $$ and $BODY$ quoting, so we use different tags here
    s"""CREATE OR REPLACE PROCEDURE ${info.schema.quoted}.${name(info).quoted}() LANGUAGE plpgsql AS
       |  $$RETIRE$$
       |    DECLARE
       |      cur_max text;
       |      prev_max text;
       |    BEGIN
       |      -- check if the previous generation has already been retired
       |      IF to_regclass(${literal(prev.tables.view.name.qualified)}) IS NULL THEN
       |        RETURN;
       |      END IF;
       |      -- note: everything else runs in a block, so that an error doesn't fail partition maintenance for the
       |      -- current generation
       |      BEGIN
       |        -- age-off is configured per generation, so apply our current configuration to the previous generation, in
       |        -- case it has been changed directly in the user data table
       |        -- note: we only write to the user data table if the configuration is out of sync, to avoid taking row locks
       |        SELECT value INTO cur_max FROM $userData WHERE type_name = ${literal(info.typeName)} AND key = $maxPartitions;
       |        SELECT value INTO prev_max FROM $userData WHERE type_name = ${literal(prev.typeName)} AND key = $maxPartitions;
       |        IF cur_max IS NULL AND prev_max IS NOT NULL THEN
       |          DELETE FROM $userData WHERE type_name = ${literal(prev.typeName)} AND key = $maxPartitions;
       |        ELSIF cur_max IS DISTINCT FROM prev_max THEN
       |          INSERT INTO $userData (type_name, key, value, mutable)
       |            SELECT ${literal(prev.typeName)}, c.key, c.value, c.mutable FROM $userData c
       |              WHERE c.type_name = ${literal(info.typeName)} AND c.key = $maxPartitions
       |            ON CONFLICT (type_name, key) DO UPDATE SET value = EXCLUDED.value;
       |        END IF;
       |        -- the previous generation has to retire any generations that came before it first
       |        IF EXISTS (SELECT FROM $userData
       |            WHERE type_name = ${literal(prev.typeName)} AND key = ${literal(SftUserData.PreviousView.key)}) THEN
       |          RETURN;
       |        END IF;
       |        -- wait until all the data in the previous generation has aged off
       |        IF $notEmpty THEN
       |          RETURN;
       |        END IF;
       |        -- don't block queries if we can't get a lock on the main view, we'll try again next time
       |        SET LOCAL lock_timeout = '1s';
       |        -- block any writes to the previous generation, and wait for any that are in progress, then
       |        -- check again to make sure that nothing was written since our first check
       |        LOCK TABLE ${tables.map(_.name.qualified).mkString(", ")} IN SHARE ROW EXCLUSIVE MODE;
       |        IF NOT ($notEmpty) THEN
       |${statements.map(s => s"          EXECUTE $$STATEMENT$$$s$$STATEMENT$$;").mkString("\n")}
       |          DELETE FROM $userData
       |            WHERE type_name = ${literal(info.typeName)} AND key = ${literal(SftUserData.PreviousView.key)};
       |          RAISE NOTICE 'Retired previous generation %', ${literal(prev.typeName)};
       |        END IF;
       |      EXCEPTION
       |        WHEN lock_not_available OR deadlock_detected THEN
       |          RAISE INFO 'Could not acquire locks to retire previous generation %, will retry', ${literal(prev.typeName)};
       |        WHEN OTHERS THEN
       |          -- don't fail partition maintenance for the current generation - for example, if a user-created view
       |          -- depends on the previous main view, then it can't be dropped until that view is removed
       |          RAISE WARNING 'Could not retire previous generation %, will retry: % (%)',
       |            ${literal(prev.typeName)}, SQLERRM, SQLSTATE;
       |      END;
       |      COMMIT;
       |    END;
       |  $$RETIRE$$;
       |""".stripMargin
  }
}
