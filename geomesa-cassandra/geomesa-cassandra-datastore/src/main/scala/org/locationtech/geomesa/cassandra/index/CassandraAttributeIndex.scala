/***********************************************************************
 * Copyright (c) 2017 IBM
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.index

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import org.locationtech.geomesa.cassandra.data._
import org.locationtech.geomesa.cassandra.{NamedColumn, RowRange, RowValue}
import org.locationtech.geomesa.index.index.AttributeIndex
import org.locationtech.geomesa.index.utils.SplitArrays
import org.opengis.feature.simple.SimpleFeatureType

case object CassandraAttributeIndex
    extends AttributeIndex[CassandraDataStore, CassandraFeature, Seq[RowValue], Seq[RowRange]]
    with CassandraFeatureIndex {

  override val version: Int = 1

  override protected def getShards(sft: SimpleFeatureType): IndexedSeq[Array[Byte]] = SplitArrays.EmptySplits

  private val Index     = NamedColumn("attrIdx",   0, "smallint", classOf[Short],  partition = true)
  private val Value     = NamedColumn("attrVal",   1, "text",     classOf[String])
  private val Secondary = NamedColumn("secondary", 2, "blob",     classOf[ByteBuffer])
  private val FeatureId = NamedColumn("fid",       3, "text",     classOf[String])

  override protected val columns: Seq[NamedColumn] = Seq(Index, Value, Secondary, FeatureId)

  //  * - 1 byte identifying the sft (OPTIONAL - only if table is shared)
  //  * - 2 bytes storing the index of the attribute in the sft
  //  * - n bytes storing the lexicoded attribute value
  //  * - NULLBYTE as a separator
  //  * - n bytes storing the secondary z-index of the feature - identified by getSecondaryIndexKeyLength
  //  * - n bytes storing the feature ID

  override protected def rowToColumns(sft: SimpleFeatureType, row: Array[Byte]): Seq[RowValue] = {

    val index = Short.box(AttributeIndex.bytesToIndex(row(0), row(1)))
    var offset = 2
    var lexicoded: String = null
    var secondaryIndex: ByteBuffer = null
    var featureId: String = null

    if (offset < row.length) {
      val nullByte = {
        val i = row.indexOf(AttributeIndex.NullByte, offset)
        if (i == -1) { row.length } else { i }
      }
      lexicoded = new String(row, offset, nullByte - offset, StandardCharsets.UTF_8)
      offset = nullByte + 1
      val secondaryIndexLength = getSecondaryIndexKeyLength(sft)
      if (offset + secondaryIndexLength < row.length) {
        secondaryIndex = ByteBuffer.wrap(row, offset, secondaryIndexLength)
        offset += secondaryIndexLength
        if (offset < row.length) {
          featureId = new String(row, offset, row.length - offset, StandardCharsets.UTF_8)
        }
      }
    }

    Seq(RowValue(Index, index), RowValue(Value, lexicoded),
      RowValue(Secondary, secondaryIndex), RowValue(FeatureId, featureId))
  }

  override protected def columnsToRow(columns: Seq[RowValue]): Array[Byte] = {
    val attributeIndex = AttributeIndex.indexToBytes(columns.head.value.asInstanceOf[Short])
    val lexicodedValue = columns(1).value.asInstanceOf[String].getBytes(StandardCharsets.UTF_8)
    val secondaryIndex = columns(2).value.asInstanceOf[ByteBuffer]
    val fid = columns(3).value.asInstanceOf[String].getBytes(StandardCharsets.UTF_8)

    val row = Array.ofDim[Byte](3 + lexicodedValue.length + secondaryIndex.remaining() + fid.length)

    row(0) = attributeIndex(0)
    row(1) = attributeIndex(1)
    System.arraycopy(lexicodedValue, 0, row, 2, lexicodedValue.length)
    var offset = 2 + lexicodedValue.length
    row(offset) = AttributeIndex.NullByte
    offset += 1
    val secondaryIndexLength = secondaryIndex.limit()
    secondaryIndex.get(row, offset, secondaryIndexLength)
    offset += secondaryIndexLength
    System.arraycopy(fid, 0, row, offset, fid.length)

    row
  }
}
