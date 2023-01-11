/***********************************************************************
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect
package tables

/**
 * Write ahead log, partitioned using inheritance to avoid any restraints on data overlap between partitions.
 * All writes get directed to the main partition, which is identified with the suffix `_writes`. Every 10 minutes,
 * the current writes partition is renamed based on a sequence number, and a new writes partition is created
 */
object WriteAheadTable extends SqlStatements {

  override protected def createStatements(info: TypeInfo): Seq[String] = {
    val table = info.tables.writeAhead
    val (tableTs, indexTs) = table.tablespace match {
      case None => ("", "")
      case Some(ts) => (s" TABLESPACE ${ts.quoted}", s" USING INDEX TABLESPACE ${ts.quoted}")
    }
    val move = table.tablespace.toSeq.map { ts =>
      s"ALTER TABLE ${table.name.qualified} SET TABLESPACE ${ts.quoted};\n"
    }
    val block =
      s"""DO $$$$
         |DECLARE
         |  seq_val smallint;
         |  partition text;
         |BEGIN
         |  SELECT value from ${info.schema.quoted}.${SequenceTable.Name.quoted}
         |    WHERE type_name = ${literal(info.typeName)} INTO seq_val;
         |  partition := ${literal(table.name.raw + "_")} || lpad(seq_val::text, 3, '0');
         |
         |  EXECUTE 'CREATE TABLE IF NOT EXISTS ${info.schema.quoted}.' || quote_ident(partition) || '(' ||
         |    'CONSTRAINT ' || quote_ident(partition || '_pkey') ||
         |    ' PRIMARY KEY (fid, ${info.cols.dtg.quoted})$indexTs ' ||
         |    ') INHERITS (${table.name.qualified})${table.storage.opts}$tableTs';
         |  EXECUTE 'CREATE INDEX IF NOT EXISTS ' || quote_ident(partition || '_' || ${info.cols.dtg.asLiteral}) ||
         |    ' ON ${info.schema.quoted}.' || quote_ident(partition) || ' (${info.cols.dtg.quoted})$tableTs';
         |${info.cols.geoms.map { col =>
      s"""  EXECUTE 'CREATE INDEX IF NOT EXISTS ' || quote_ident(partition || '_spatial_' || ${col.asLiteral}) ||
         |    ' ON ${info.schema.quoted}.' || quote_ident(partition) || ' USING gist(${col.quoted})$tableTs';""".stripMargin}.mkString("\n")}
         |${info.cols.indexed.map { col =>
      s"""  EXECUTE 'CREATE INDEX IF NOT EXISTS ' || quote_ident(partition || '_' || ${col.asLiteral}) ||
         |    ' ON ${info.schema.quoted}.' || quote_ident(partition) || '(${col.quoted})$tableTs';""".stripMargin}.mkString("\n")}
         |END $$$$;""".stripMargin

    move ++ Seq(block)
  }

  override protected def dropStatements(info: TypeInfo): Seq[String] = {
    Seq(
      s"DROP TABLE IF EXISTS ${info.tables.writeAhead.name.qualified};", // note: actually gets dropped by jdbc store
      s"DROP SEQUENCE IF EXISTS ${escape(info.tables.writeAhead.name.raw, "seq")};"
    )
  }
}
