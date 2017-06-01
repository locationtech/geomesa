/***********************************************************************
 * Copyright (c) 2017 IBM
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.index


import java.nio.charset.StandardCharsets

import org.locationtech.geomesa.cassandra._
import org.locationtech.geomesa.cassandra.data._
import org.locationtech.geomesa.index.index.IdIndex
import org.opengis.feature.simple.SimpleFeatureType

case object CassandraIdIndex
    extends IdIndex[CassandraDataStore, CassandraFeature, Seq[RowValue], Seq[RowRange]]
    with CassandraFeatureIndex {

  override val version: Int = 1

  private val FeatureId = NamedColumn("fid", 0, "text", classOf[String], partition = true)

  override protected val columns: Seq[NamedColumn] = Seq(FeatureId)

  override protected def rowToColumns(sft: SimpleFeatureType, row: Array[Byte]): Seq[RowValue] = {
    val featureId = if (row.length > 0) {
      new String(row, 0, row.length, StandardCharsets.UTF_8)
    } else {
      null
    }
    Seq(RowValue(FeatureId, featureId))
  }

  override protected def columnsToRow(columns: Seq[RowValue]): Array[Byte] =
    columns.head.value.asInstanceOf[String].getBytes(StandardCharsets.UTF_8)
}
