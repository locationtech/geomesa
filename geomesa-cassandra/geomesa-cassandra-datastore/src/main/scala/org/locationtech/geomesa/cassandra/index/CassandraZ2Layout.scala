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

import com.google.common.primitives.Longs
import org.locationtech.geomesa.cassandra.{NamedColumn, RowValue}
import org.opengis.feature.simple.SimpleFeatureType

trait CassandraZ2Layout extends CassandraFeatureIndex {

  private val Shard     = NamedColumn("shard", 0, "tinyint",  classOf[Byte],   partition = true)
  private val ZValue    = NamedColumn("z",     1, "bigint",   classOf[Long])
  private val FeatureId = NamedColumn("fid",   2, "text",     classOf[String])

  override protected val columns: Seq[NamedColumn] = Seq(Shard, ZValue, FeatureId)

  //  * - 1 byte identifying the sft (OPTIONAL - only if table is shared)
  //  * - 1 byte shard
  //  * - 8 bytes z value
  //  * - n bytes feature ID

  override protected def rowToColumns(sft: SimpleFeatureType, row: Array[Byte]): Seq[RowValue] = {
    import CassandraFeatureIndex.RichByteArray

    var shard: java.lang.Byte = null
    var z: java.lang.Long = null
    var fid: String = null

    if (row.length > 0) {
      shard = row(0)
      if (row.length > 1) {
        z = Longs.fromBytes(row(1), row.getOrElse(2, 0), row.getOrElse(3, 0), row.getOrElse(4, 0), row.getOrElse(5, 0),
          row.getOrElse(6, 0), row.getOrElse(7, 0), row.getOrElse(8, 0))
        if (row.length > 9) {
          fid = new String(row, 9, row.length - 9, StandardCharsets.UTF_8)
        }
      }
    }

    Seq(RowValue(Shard, shard), RowValue(ZValue, z), RowValue(FeatureId, fid))
  }

  override protected def columnsToRow(columns: Seq[RowValue]): Array[Byte] = {
    val shard = columns.head.value.asInstanceOf[Byte]
    val z = Longs.toByteArray(columns(1).value.asInstanceOf[Long])
    val fid = columns(2).value.asInstanceOf[String].getBytes(StandardCharsets.UTF_8)

    val row = Array.ofDim[Byte](9 + fid.length)

    row(0) = shard
    System.arraycopy(z, 0, row, 1, 8)
    System.arraycopy(fid, 0, row, 9, fid.length)

    row
  }
}
