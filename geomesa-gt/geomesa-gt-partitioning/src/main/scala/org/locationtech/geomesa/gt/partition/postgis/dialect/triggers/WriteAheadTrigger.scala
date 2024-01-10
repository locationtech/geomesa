/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect
package triggers

import org.locationtech.geomesa.gt.partition.postgis.dialect.tables.SequenceTable

/**
 * Trigger to delegate writes from the write ahead table to the _writes partition
 */
object WriteAheadTrigger extends SqlTriggerFunction {

  override def name(info: TypeInfo): FunctionName = FunctionName(s"insert_to_wa_writes_${info.typeName}")

  override protected def table(info: TypeInfo): TableIdentifier = info.tables.writeAhead.name

  override protected def action: String = "BEFORE INSERT"

  override protected def createStatements(info: TypeInfo): Seq[String] =
    Seq(function(info)) ++ super.createStatements(info)

  private def function(info: TypeInfo): String =
    s"""CREATE OR REPLACE FUNCTION ${name(info).quoted}() RETURNS trigger AS
       |  $$BODY$$
       |    DECLARE
       |      seq_val smallint;
       |      partition text;
       |    BEGIN
       |      SELECT value from ${info.schema.quoted}.${SequenceTable.Name.quoted}
       |        WHERE type_name = ${literal(info.typeName)} INTO seq_val;
       |      partition := ${literal(info.tables.writeAhead.name.raw + "_")} || lpad(seq_val::text, 3, '0');
       |      EXECUTE 'INSERT INTO ${info.schema.quoted}.' || quote_ident(partition) ||
       |        ' VALUES ($$1.*) ON CONFLICT DO NOTHING'
       |        USING NEW;
       |      RETURN NULL;
       |    END;
       |  $$BODY$$
       |LANGUAGE plpgsql;""".stripMargin
}
