/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect
package triggers

/**
 * Trigger to delegate updates from the main view to the sub tables
 */
object UpdateTrigger extends SqlTriggerFunction {

  override def name(info: TypeInfo): String = s"update_to_${info.name}"

  override protected def table(info: TypeInfo): TableName = info.tables.view.name

  override protected def action: String = "INSTEAD OF UPDATE"

  override protected def createStatements(info: TypeInfo): Seq[String] =
    Seq(function(info)) ++ super.createStatements(info)

  private def function(info: TypeInfo): String = {
    val updateFields = info.cols.all.map(c => s"${c.name} = NEW.${c.name}").mkString(",")
    val where = s"fid = OLD.fid AND ${info.cols.dtg.name} = OLD.${info.cols.dtg.name}"

    s"""CREATE OR REPLACE FUNCTION "${name(info)}"() RETURNS trigger AS
       |  $$BODY$$
       |    DECLARE
       |      del_count integer;
       |    BEGIN
       |      UPDATE ${info.tables.writeAhead.name.full} SET $updateFields WHERE $where;
       |      IF NOT FOUND THEN
       |        DELETE FROM ${info.tables.writeAheadPartitions.name.full} WHERE $where;
       |        GET DIAGNOSTICS del_count := ROW_COUNT;
       |        IF del_count > 0 THEN
       |          INSERT INTO ${info.tables.writeAhead.name.full} VALUES(NEW.*);
       |        ELSE
       |          DELETE FROM ${info.tables.mainPartitions.name.full} WHERE $where;
       |          GET DIAGNOSTICS del_count := ROW_COUNT;
       |          IF del_count > 0 THEN
       |            INSERT INTO ${info.tables.writeAhead.name.full} VALUES(NEW.*);
       |          ELSE
       |            RETURN NULL;
       |          END IF;
       |        END IF;
       |      END IF;
       |      RETURN NEW;
       |    END;
       |  $$BODY$$
       |LANGUAGE plpgsql VOLATILE
       |COST 100;""".stripMargin
  }
}
