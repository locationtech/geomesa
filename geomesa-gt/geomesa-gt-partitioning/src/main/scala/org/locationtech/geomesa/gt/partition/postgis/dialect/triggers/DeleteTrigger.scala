/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect
package triggers

/**
 * Trigger to delegate deletes from the main view to the sub tables
 */
object DeleteTrigger extends SqlTriggerFunction {

  override def name(info: TypeInfo): FunctionName = FunctionName(s"delete_from_${info.typeName}")

  override protected def table(info: TypeInfo): TableIdentifier = info.tables.view.name

  override protected def action: String = "INSTEAD OF DELETE"

  override protected def createStatements(info: TypeInfo): Seq[String] =
    Seq(function(info)) ++ super.createStatements(info)

  private def function(info: TypeInfo): String = {
    def delete(table: TableConfig): String =
      s"DELETE FROM ${table.name.qualified} WHERE fid = OLD.fid AND ${info.cols.dtg.quoted} = OLD.${info.cols.dtg.quoted}"

    s"""CREATE OR REPLACE FUNCTION ${name(info).quoted}() RETURNS trigger AS
       |  $$BODY$$
       |    DECLARE
       |      del_count integer;
       |    BEGIN
       |      ${delete(info.tables.writeAhead)};
       |      GET DIAGNOSTICS del_count := ROW_COUNT;
       |      IF del_count = 0 THEN
       |        ${delete(info.tables.writeAheadPartitions)};
       |        GET DIAGNOSTICS del_count := ROW_COUNT;
       |        IF del_count = 0 THEN
       |          ${delete(info.tables.mainPartitions)};
       |          GET DIAGNOSTICS del_count := ROW_COUNT;
       |          IF del_count = 0 THEN
       |            ${delete(info.tables.spillPartitions)};
       |            GET DIAGNOSTICS del_count := ROW_COUNT;
       |            IF del_count = 0 THEN
       |              RETURN NULL;
       |            END IF;
       |          END IF;
       |        END IF;
       |      END IF;
       |      RETURN OLD;
       |    END;
       |  $$BODY$$
       |LANGUAGE plpgsql VOLATILE
       |COST 100;""".stripMargin
  }
}
