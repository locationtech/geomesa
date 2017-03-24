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

import com.google.common.primitives.{Longs, Shorts}
import org.locationtech.geomesa.cassandra.{NamedColumn, RowValue}
import org.opengis.feature.simple.SimpleFeatureType

trait CassandraZ3Layout extends CassandraFeatureIndex {

  private val Shard     = NamedColumn("shard",  0, "tinyint",  classOf[Byte],   partition = true)
  private val Period    = NamedColumn("period", 1, "smallint", classOf[Short],  partition = true)
  private val ZValue    = NamedColumn("z",      2, "bigint",   classOf[Long])
  private val FeatureId = NamedColumn("fid",    3, "text",     classOf[String])

  override protected val columns: Seq[NamedColumn] = Seq(Shard, Period, ZValue, FeatureId)

  //  * - 1 byte identifying the sft (OPTIONAL - only if table is shared)
  //  * - 1 byte shard
  //  * - 2 byte period
  //  * - 8 bytes z value
  //  * - n bytes feature ID

  override protected def rowToColumns(sft: SimpleFeatureType, row: Array[Byte]): Seq[RowValue] = {
    import CassandraFeatureIndex.RichByteArray

    var shard: java.lang.Byte = null
    var period: java.lang.Short = null
    var z: java.lang.Long = null
    var fid: String = null
    if (row.length > 0) {
      shard = row(0)
      if (row.length > 1) {
        period = Shorts.fromBytes(row(1), row(2))
        if (row.length > 3) {
          z = Longs.fromBytes(row(3), row.getOrElse(4, 0), row.getOrElse(5, 0), row.getOrElse(6, 0),
            row.getOrElse(7, 0), row.getOrElse(8, 0), row.getOrElse(9, 0), row.getOrElse(10, 0))
          if (row.length > 11) {
            fid = new String(row, 11, row.length - 11, StandardCharsets.UTF_8)
          }
        }
      }
    }

    Seq(RowValue(Shard, shard), RowValue(Period, period), RowValue(ZValue, z), RowValue(FeatureId, fid))
  }

  override protected def columnsToRow(columns: Seq[RowValue]): Array[Byte] = {
    val shard = columns.head.value.asInstanceOf[Byte]
    val period = Shorts.toByteArray(columns(1).value.asInstanceOf[Short])
    val z = Longs.toByteArray(columns(2).value.asInstanceOf[Long])
    val fid = columns(3).value.asInstanceOf[String].getBytes(StandardCharsets.UTF_8)

    val row = Array.ofDim[Byte](11 + fid.length)

    row(0) = shard
    System.arraycopy(period, 0, row, 1, 2)
    System.arraycopy(z, 0, row, 3, 8)
    System.arraycopy(fid, 0, row, 11, fid.length)

    row
  }
}
