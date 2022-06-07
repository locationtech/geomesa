/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 7d3c180a16 (GEOMESA-3209 Postgis - allow for re-creation of schema if _wa table exists)
>>>>>>> 1116449a74 (GEOMESA-3209 Postgis - allow for re-creation of schema if _wa table exists)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7e938f4284 (GEOMESA-3209 Postgis - allow for re-creation of schema if _wa table exists)
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d43301c50b (GEOMESA-3209 Postgis - allow for re-creation of schema if _wa table exists)
=======
>>>>>>> b8634a9aef (GEOMESA-3209 Postgis - allow for re-creation of schema if _wa table exists)
=======
>>>>>>> 7d3c180a16 (GEOMESA-3209 Postgis - allow for re-creation of schema if _wa table exists)
=======
>>>>>>> 1ba7d8749d (GEOMESA-3209 Postgis - allow for re-creation of schema if _wa table exists)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e39e1eda4e (GEOMESA-3209 Postgis - allow for re-creation of schema if _wa table exists)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 7d3c180a16 (GEOMESA-3209 Postgis - allow for re-creation of schema if _wa table exists)
>>>>>>> eab5f5c2d7 (GEOMESA-3209 Postgis - allow for re-creation of schema if _wa table exists)
=======
=======
>>>>>>> 7e938f4284 (GEOMESA-3209 Postgis - allow for re-creation of schema if _wa table exists)
=======
=======
>>>>>>> 478b92b696 (GEOMESA-3209 Postgis - allow for re-creation of schema if _wa table exists)
=======
>>>>>>> d43301c50b (GEOMESA-3209 Postgis - allow for re-creation of schema if _wa table exists)
=======
>>>>>>> e39e1eda4e (GEOMESA-3209 Postgis - allow for re-creation of schema if _wa table exists)
=======
>>>>>>> b8634a9aef (GEOMESA-3209 Postgis - allow for re-creation of schema if _wa table exists)
    val partition = writesPartition(info).qualified
    val seq = s"CREATE SEQUENCE IF NOT EXISTS ${escape(table.name.raw, "seq")} AS smallint MINVALUE 1 MAXVALUE 999 CYCLE;"
    // rename the table created by the JdbcDataStore to be the write ahead table
    val rename =
      s"""DO $$$$
         |BEGIN
         |  IF NOT EXISTS (SELECT FROM pg_tables WHERE schemaname = ${info.schema.asLiteral} AND tablename = ${table.name.asLiteral}) THEN
         |    ALTER TABLE ${info.tables.view.name.qualified} RENAME TO ${table.name.quoted};
         |  ELSE
         |    DROP TABLE ${info.tables.view.name.qualified};
         |  END IF;
         |END$$$$;""".stripMargin
    // drop the index created by the JDBC data store on the parent table, as all the data is in the inherited table
    val dropIndices = info.cols.geoms.map { col =>
      s"DROP INDEX IF EXISTS ${escape("spatial", info.tables.view.name.raw, col.raw.toLowerCase(Locale.US))};"
    }
    val move = table.tablespace.toSeq.map { ts =>
      s"ALTER TABLE ${table.name.qualified} SET TABLESPACE ${ts.quoted};"
    }
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 50746157c5 (GEOMESA-3209 Postgis - allow for re-creation of schema if _wa table exists)
=======
>>>>>>> eab5f5c2d7 (GEOMESA-3209 Postgis - allow for re-creation of schema if _wa table exists)
=======
>>>>>>> 7e938f4284 (GEOMESA-3209 Postgis - allow for re-creation of schema if _wa table exists)
=======
>>>>>>> 27b2ae4d07 (GEOMESA-3209 Postgis - allow for re-creation of schema if _wa table exists)
=======
>>>>>>> 1116449a74 (GEOMESA-3209 Postgis - allow for re-creation of schema if _wa table exists)
=======
>>>>>>> d43301c50b (GEOMESA-3209 Postgis - allow for re-creation of schema if _wa table exists)
=======
>>>>>>> b8634a9aef (GEOMESA-3209 Postgis - allow for re-creation of schema if _wa table exists)
=======
=======
>>>>>>> 50746157c5 (GEOMESA-3209 Postgis - allow for re-creation of schema if _wa table exists)
>>>>>>> b04e08ea20 (GEOMESA-3209 Postgis - allow for re-creation of schema if _wa table exists)
>>>>>>> c61a3b395b (GEOMESA-3209 Postgis - allow for re-creation of schema if _wa table exists)
=======
>>>>>>> c61a3b395 (GEOMESA-3209 Postgis - allow for re-creation of schema if _wa table exists)
>>>>>>> cdf6c5eb97 (GEOMESA-3209 Postgis - allow for re-creation of schema if _wa table exists)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b04e08ea20 (GEOMESA-3209 Postgis - allow for re-creation of schema if _wa table exists)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c61a3b395 (GEOMESA-3209 Postgis - allow for re-creation of schema if _wa table exists)
>>>>>>> 7d3c180a16 (GEOMESA-3209 Postgis - allow for re-creation of schema if _wa table exists)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c61a3b395 (GEOMESA-3209 Postgis - allow for re-creation of schema if _wa table exists)
>>>>>>> 1ba7d8749d (GEOMESA-3209 Postgis - allow for re-creation of schema if _wa table exists)
=======
>>>>>>> c61a3b395b (GEOMESA-3209 Postgis - allow for re-creation of schema if _wa table exists)
>>>>>>> e39e1eda4e (GEOMESA-3209 Postgis - allow for re-creation of schema if _wa table exists)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 50746157c5 (GEOMESA-3209 Postgis - allow for re-creation of schema if _wa table exists)
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> c61a3b395 (GEOMESA-3209 Postgis - allow for re-creation of schema if _wa table exists)
>>>>>>> 7d3c180a16 (GEOMESA-3209 Postgis - allow for re-creation of schema if _wa table exists)
>>>>>>> eab5f5c2d7 (GEOMESA-3209 Postgis - allow for re-creation of schema if _wa table exists)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> c61a3b395 (GEOMESA-3209 Postgis - allow for re-creation of schema if _wa table exists)
>>>>>>> 1ba7d8749d (GEOMESA-3209 Postgis - allow for re-creation of schema if _wa table exists)
>>>>>>> 7e938f4284 (GEOMESA-3209 Postgis - allow for re-creation of schema if _wa table exists)
=======
>>>>>>> c61a3b395b (GEOMESA-3209 Postgis - allow for re-creation of schema if _wa table exists)
>>>>>>> 478b92b696 (GEOMESA-3209 Postgis - allow for re-creation of schema if _wa table exists)
=======
>>>>>>> 27b2ae4d07 (GEOMESA-3209 Postgis - allow for re-creation of schema if _wa table exists)
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> c61a3b395 (GEOMESA-3209 Postgis - allow for re-creation of schema if _wa table exists)
>>>>>>> 7d3c180a16 (GEOMESA-3209 Postgis - allow for re-creation of schema if _wa table exists)
>>>>>>> 1116449a74 (GEOMESA-3209 Postgis - allow for re-creation of schema if _wa table exists)
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> c61a3b395 (GEOMESA-3209 Postgis - allow for re-creation of schema if _wa table exists)
>>>>>>> 1ba7d8749d (GEOMESA-3209 Postgis - allow for re-creation of schema if _wa table exists)
>>>>>>> d43301c50b (GEOMESA-3209 Postgis - allow for re-creation of schema if _wa table exists)
=======
>>>>>>> b8634a9aef (GEOMESA-3209 Postgis - allow for re-creation of schema if _wa table exists)
=======
=======
>>>>>>> 50746157c5 (GEOMESA-3209 Postgis - allow for re-creation of schema if _wa table exists)
>>>>>>> b04e08ea20 (GEOMESA-3209 Postgis - allow for re-creation of schema if _wa table exists)
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
