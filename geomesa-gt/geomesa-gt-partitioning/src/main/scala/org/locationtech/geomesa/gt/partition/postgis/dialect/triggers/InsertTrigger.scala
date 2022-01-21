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
 * Trigger to delegate inserts from the main view to the sub tables
 */
object InsertTrigger extends SqlTriggerFunction {

  override def name(info: TypeInfo): String = s"insert_to_${info.name}"

  override protected def table(info: TypeInfo): TableName = info.tables.view.name

  override protected def action: String = "INSTEAD OF INSERT"

  override protected def createStatements(info: TypeInfo): Seq[String] =
    Seq(function(info)) ++ super.createStatements(info)

  private def function(info: TypeInfo): String =
    s"""CREATE OR REPLACE FUNCTION "${name(info)}"() RETURNS trigger AS
       |  $$BODY$$
       |    BEGIN
       |      INSERT INTO ${info.tables.writeAhead.name.full} VALUES(NEW.*);
       |      RETURN NEW;
       |    END;
       |  $$BODY$$
       |LANGUAGE plpgsql VOLATILE
       |COST 100;""".stripMargin
}
