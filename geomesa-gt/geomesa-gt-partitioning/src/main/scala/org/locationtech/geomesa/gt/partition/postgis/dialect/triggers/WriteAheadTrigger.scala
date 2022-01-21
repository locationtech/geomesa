/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect
package triggers

import org.locationtech.geomesa.gt.partition.postgis.dialect.tables.WriteAheadTable

/**
 * Trigger to delegate writes from the write ahead table to the _writes partition
 */
object WriteAheadTrigger extends SqlTriggerFunction {

  override def name(info: TypeInfo): String = s"insert_to_wa_writes_${info.name}"

  override protected def table(info: TypeInfo): TableName = info.tables.writeAhead.name

  override protected def action: String = "BEFORE INSERT"

  override protected def createStatements(info: TypeInfo): Seq[String] =
    Seq(function(info)) ++ super.createStatements(info)

  private def function(info: TypeInfo): String =
    s"""CREATE OR REPLACE FUNCTION "${name(info)}"() RETURNS trigger AS
       |  $$BODY$$
       |    BEGIN
       |      INSERT INTO ${WriteAheadTable.writesPartition(info)} VALUES (NEW.*) ON CONFLICT DO NOTHING;
       |      RETURN NULL;
       |    END;
       |  $$BODY$$
       |LANGUAGE plpgsql;""".stripMargin
}
