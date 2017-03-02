/***********************************************************************
* Copyright (c) 2013-2016 IBM
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.cassandra.index

import com.datastax.driver.core._
import com.google.common.primitives.{Bytes, Longs}
import org.locationtech.geomesa.cassandra.data._
import org.locationtech.geomesa.index.index.ClientSideFiltering.RowAndValue
import org.locationtech.geomesa.index.index.XZ2Index

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

case object CassandraXZ2Index
    extends CassandraFeatureIndex with XZ2Index[CassandraDataStore, CassandraFeature, CassandraRow, CassandraRow] {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  override val version: Int = 1

  // TODO this is the same as CassandraZ2Index, make utility
  override def getRowValuesFromSFT(sft: SimpleFeatureType): RowToValues = {
    val sharing = sft.getTableSharingPrefix
    (row) => {
      var curr = if (sharing == "") 0 else 1
      val pk:java.lang.Integer = row(curr) & 0xFF
      curr += 1
      val z = ByteBuffer.wrap(row, curr, 8).getLong
      curr += 8
      val fid = new String(row, curr, row.length - curr, StandardCharsets.UTF_8)
      (pk, z, fid, sharing)
    }
  }

  override protected def rowAndValue(result: Row): RowAndValue = {
    val pk = result.getInt(0).byteValue
    val sharing = result.getString(1)
    val z = result.getLong(2)
    val fid:Array[Byte] = result.getString(3).getBytes
    val sf:Array[Byte] = result.getBytes(4).array

    val row = if (sharing == "") {
      Bytes.concat(Array(pk), Longs.toByteArray(z), fid)
    } else {
      Bytes.concat(sharing.getBytes, Array(pk), Longs.toByteArray(z), fid)
    }
    RowAndValue(row, 0, row.length, sf, 0, sf.length)
  }
}
