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
 * Main view of all the partitions and write ahead table. This should accept and reads and writes.
 */
object MainView extends SqlStatements {

  override protected def createStatements(info: TypeInfo): Seq[String] = {
    Seq(
      s"""CREATE OR REPLACE VIEW ${info.tables.view.name.qualified} AS
         |  SELECT * FROM ${info.tables.writeAhead.name.qualified} UNION ALL
         |  SELECT * FROM ${info.tables.writeAheadPartitions.name.qualified} UNION ALL
         |  SELECT * FROM ${info.tables.mainPartitions.name.qualified} UNION ALL
         |  SELECT * FROM ${info.tables.spillPartitions.name.qualified};""".stripMargin
    )
  }

  override protected def dropStatements(info: TypeInfo): Seq[String] =
    Seq(s"DROP VIEW IF EXISTS ${info.tables.view.name.qualified};")
}
