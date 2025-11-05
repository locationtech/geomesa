/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect
package triggers

/**
 * Trigger to delegate inserts from the main view to the sub tables
 */
object InsertTrigger extends SqlTriggerFunction {

  override def name(info: TypeInfo): FunctionName = FunctionName(s"insert_to_${info.typeName}")

  override protected def table(info: TypeInfo): TableIdentifier = info.tables.view.name

  override protected def action: String = "INSTEAD OF INSERT"

  override protected def createStatements(info: TypeInfo): Seq[String] =
    Seq(function(info)) ++ super.createStatements(info)

  private def function(info: TypeInfo): String =
    s"""CREATE OR REPLACE FUNCTION ${info.schema.quoted}.${name(info).quoted}() RETURNS trigger AS
       |  $$BODY$$
       |    BEGIN
       |      INSERT INTO ${info.tables.writeAhead.name.qualified} VALUES(NEW.*);
       |      RETURN NEW;
       |    END;
       |  $$BODY$$
       |LANGUAGE plpgsql VOLATILE
       |COST 100;""".stripMargin
}
