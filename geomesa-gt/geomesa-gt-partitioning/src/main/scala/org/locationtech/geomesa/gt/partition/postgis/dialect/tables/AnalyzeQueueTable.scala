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
 * Stores tables that need to be analyzed
 */
object AnalyzeQueueTable extends SqlStatements {

  override protected def createStatements(info: TypeInfo): Seq[String] = {
    val create =
      s"""CREATE TABLE IF NOT EXISTS ${info.tables.analyzeQueue.name.qualified} (
         |  partition_name text,
         |  enqueued timestamp without time zone
         |);""".stripMargin
    Seq(create)
  }

  override protected def dropStatements(info: TypeInfo): Seq[String] =
    Seq(s"DROP TABLE IF EXISTS ${info.tables.analyzeQueue.name.qualified};")
}
