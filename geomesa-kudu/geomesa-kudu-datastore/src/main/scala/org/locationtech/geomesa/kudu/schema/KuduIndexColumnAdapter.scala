/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.schema

import org.apache.kudu.ColumnSchema.{ColumnSchemaBuilder, CompressionAlgorithm, Encoding}
import org.apache.kudu.client.{PartialRow, RowResult}
import org.apache.kudu.{ColumnSchema, Type}
import org.locationtech.geomesa.kudu.KuduSystemProperties
import org.locationtech.geomesa.kudu.schema.KuduSimpleFeatureSchema.KuduFilter
import org.opengis.filter.Filter

/**
  * Index column adapter - single column, defined as a primary key
  *
  * note: bit-shuffle encoding is already compressed, so don't compress again
  *
  * @param name name
  * @param typed column type
  * @param encoding column encoding
  * @param compression column compression
  * @tparam T type binding
  */
abstract class KuduIndexColumnAdapter[T](val name: String,
                                         typed: Type,
                                         encoding: Encoding,
                                         compression: CompressionAlgorithm,
                                         key: Boolean = true) extends KuduColumnAdapter[T] {

  val column: ColumnSchema = {
    val builder = new ColumnSchemaBuilder(name, typed).nullable(!key).key(key)
    builder.encoding(encoding)
    builder.compressionAlgorithm(compression)
    KuduSystemProperties.BlockSize.option.map(_.toInt).foreach(builder.desiredBlockSize)
    builder.build()
  }

  override val columns: Seq[ColumnSchema] = Seq(column)
  override def predicate(filter: Filter): KuduFilter = throw new NotImplementedError()
}

object KuduIndexColumnAdapter {

  import CompressionAlgorithm.NO_COMPRESSION
  import Encoding._
  import Type._
  import org.locationtech.geomesa.kudu.utils.ColumnConfiguration.compression

  // this is used as the last part of a key for uniqueness
  // use plain encoding - since these are unique, dictionary or prefix isn't likely to help
  object FeatureIdAdapter extends FeatureIdAdapter(PLAIN_ENCODING, compression())

  class FeatureIdAdapter(encoding: Encoding, compression: CompressionAlgorithm)
      extends KuduIndexColumnAdapter[String]("fid", STRING, encoding, compression) {
    override def readFromRow(row: RowResult): String = row.getString(name)
    override def writeToRow(row: PartialRow, value: String): Unit = row.addString(name, value)
  }

  object UnusedFeatureIdAdapter extends KuduColumnAdapter[String] {
    override def name: String = throw new NotImplementedError()
    override def columns: Seq[ColumnSchema] = Seq.empty
    override def readFromRow(row: RowResult): String = ""
    override def writeToRow(row: PartialRow, value: String): Unit = {}
    override def predicate(filter: Filter): KuduFilter = throw new NotImplementedError()
  }

  // use bit-shuffle encoding, which is good for values that change incrementally
  // note: bit-shuffle is automatically lz4 encoded, so we don't want to add additional compression on top
  object ZColumnAdapter extends KuduIndexColumnAdapter[Long]("z", INT64, BIT_SHUFFLE, NO_COMPRESSION) {
    override def readFromRow(row: RowResult): Long = row.getLong(name)
    override def writeToRow(row: PartialRow, value: Long): Unit = row.addLong(name, value)
  }

  // used for time period (bin) in z3/xz3
  // use run-length encoding, which is good for repeated values
  object PeriodColumnAdapter extends KuduIndexColumnAdapter[Short]("period", INT16, RLE, compression()) {
    override def readFromRow(row: RowResult): Short = row.getShort(name)
    override def writeToRow(row: PartialRow, value: Short): Unit = row.addShort(name, value)
  }

  // used for attribute index lexicoded values
  // use prefix encoding - values will be sorted
  // TODO use appropriate type instead of lexicoding?
  object ValueColumnAdapter extends KuduIndexColumnAdapter[String]("value", STRING, PREFIX_ENCODING, compression()) {
    override def readFromRow(row: RowResult): String = row.getString(name)
    override def writeToRow(row: PartialRow, value: String): Unit = row.addString(name, value)
  }

  // used for attribute index secondary tiered index
  // has to be bytes as content type can vary between sfts
  // because it's binary, can only use prefix, dictionary or plain encoding - prefix seems best
  object SecondaryColumnAdapter
      extends KuduIndexColumnAdapter[Array[Byte]]("secondary", BINARY, PREFIX_ENCODING, compression()) {
    override def readFromRow(row: RowResult): Array[Byte] = row.getBinaryCopy(name)
    override def writeToRow(row: PartialRow, value: Array[Byte]): Unit = row.addBinary(name, value)
  }

  object VisibilityAdapter extends KuduIndexColumnAdapter[String]("vis", STRING, DICT_ENCODING, compression(), false) {
    override def readFromRow(row: RowResult): String =
      if (row.isNull(name)) { null } else { row.getString(name) }
    override def writeToRow(row: PartialRow, value: String): Unit =
      if (value == null || value.isEmpty) { row.setNull(name) } else { row.addString(name, value) }
  }
}
